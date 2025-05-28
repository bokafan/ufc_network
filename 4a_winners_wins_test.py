import os
import random
import time
import pandas as pd
import subprocess
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# VPN city pool
VPN_CITIES = [
    "Atlanta", "Boston", "Buffalo", "Charlotte", "Chicago", "Dallas", "Denver",
    "Houston", "Kansas_City", "Los_Angeles", "Manassas", "McAllen", "Miami",
    "New_York", "Omaha", "Phoenix", "Saint_Louis", "Salt_Lake_City", 
    "San_Francisco", "Seattle"
]

# Load all fighter data
df = pd.read_parquet("3a_winners_combined.parquet")
fighters = df[['winner', 'winner_link']].drop_duplicates().reset_index(drop=True)
chunk_size = 350

def run_shell(cmd):
    """Run shell commands and print output."""
    print(f"\n▶️ Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

def kill_zombie_chrome_processes():
    """Kill zombie Chrome processes to avoid buildup."""
    cmd = "ps aux | grep '[c]hrome' | awk '$8==\"Z\" {print $2}' | xargs -r kill -9"
    print("\n🧹 Killing zombie Chrome processes...")
    subprocess.run(cmd, shell=True)

def init_driver():
    """Initialize a fresh Selenium Chrome driver in headless mode."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    return webdriver.Chrome(options=chrome_options)

def scrape_fighter(driver, fighter_name, fighter_url):
    """Scrape one fighter page, return list of bout dicts or empty list."""
    output = []
    
    # Retry loading the fighter page up to 2 times, restarting driver on failure
    for attempt in range(2):
        try:
            driver.get(fighter_url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "section.fighterFightResults"))
            )
            break  # success, exit retry loop
        except Exception as e:
            print(f"❌ Failed to load page (attempt {attempt + 1}): {e}")
            if attempt == 0:
                print("🚦 Restarting driver and retrying...")
                try:
                    driver.quit()
                except:
                    pass
                driver = init_driver()
                time.sleep(5)
            else:
                print(f"⚠️ Giving up on {fighter_name} after 2 attempts.")
                return output, driver  # empty list, plus possibly new driver
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    results_container = soup.select_one("section.fighterFightResults div#proResults")
    if not results_container:
        print("❌ No <div id='proResults'> found — skipping.")
        return output, driver

    bouts = results_container.select("div[data-fighter-bout-target='bout']")
    print(f"✅ Found {len(bouts)} bouts")

    for bout in bouts:
        if 'Amateur Bouts' in bout.text:
            print("🚫 Reached 'Amateur Bouts', stopping scrape for this fighter.")
            break

        result_div = bout.select_one("div.result div")
        result_text = result_div.text.strip() if result_div else ''
        if result_text != 'W':
            continue

        record_span = bout.select_one("span[title='Fighter Record Before Fight']")
        if not record_span or not re.match(r"^\d+-\d+", record_span.text.strip()):
            continue

        opponent_tag = bout.select_one('a[href*="/fighters/"]')
        opponent_name = opponent_tag.text.strip() if opponent_tag else 'null'
        opponent_link = urljoin("https://www.tapology.com", opponent_tag['href']) if opponent_tag else 'null'

        event_tag = bout.select_one('a[href*="/fightcenter/events/"]')
        event_url = urljoin("https://www.tapology.com", event_tag['href']) if event_tag else 'null'

        year_span = bout.select_one('a[href*="/fightcenter/events/"] span.font-bold')
        date_span = bout.select_one('a[href*="/fightcenter/events/"] span.text-neutral-600')
        event_year = year_span.text.strip() if year_span else 'null'
        event_md = date_span.text.strip() if date_span else 'null'

        victory_details = 'null'
        middle_col = bout.select_one("div.md\\:flex.flex-col.justify-center.gap-1\\.5")
        if middle_col:
            method_tag = middle_col.select_one("a[href*='/fightcenter/bouts/']")
            time_div = middle_col.select_one("div.text-xs11.text-neutral-600")
            method_text = method_tag.text.strip() if method_tag else ''
            time_text = time_div.get_text(strip=True) if time_div else ''
            victory_details = f"{method_text} · {time_text}".strip(' ·')

        duration = weight = odds = 'null'
        bout_id = bout.get('data-bout-id')
        if bout_id:
            detail_div = soup.select_one(f'#boutDetails{bout_id}')
            if detail_div:
                for t in detail_div.select('div.h-\\[34px\\]'):
                    label_tag = t.select_one('span.font-bold')
                    value_tag = t.select_one('span:not(.font-bold)')
                    if not label_tag or not value_tag:
                        continue
                    label = label_tag.text.strip().replace(':', '')
                    value = value_tag.text.strip()
                    if label == 'Duration':
                        duration = value
                    elif label == 'Weight':
                        weight = value
                    elif label == 'Odds':
                        odds = value

        output.append({
            'original_fighter_name': fighter_name,
            'original_fighter_url': fighter_url,
            'opponent_name': opponent_name,
            'opponent_url': opponent_link,
            'event_url': event_url,
            'event_year': event_year,
            'event_month_day': event_md,
            'duration': duration,
            'weight': weight,
            'odds': odds,
            'victory_details': victory_details,
            'result': result_text
        })

    return output, driver

def scrape_chunk(fighter_chunk, chunk_num):
    """Scrape one chunk of fighters, save results to parquet."""
    output = []

    driver = init_driver()
    for row in fighter_chunk:
        fighter_name = row['winner']
        fighter_url = urljoin("https://www.tapology.com", row['winner_link'])
        print(f"\n🔍 Scraping {fighter_name}: {fighter_url}")

        try:
            fighter_data, driver = scrape_fighter(driver, fighter_name, fighter_url)
            if fighter_data:
                output.extend(fighter_data)
                print(f"📦 Added {len(fighter_data)} row(s) for {fighter_name}")
            else:
                print(f"⚠️ No valid rows scraped for {fighter_name}")

        except Exception as e:
            print(f"❌ Error scraping {fighter_url}: {e}")

        kill_zombie_chrome_processes()  # Call cleanup after every URL scraped
        time.sleep(random.uniform(2, 8))

    driver.quit()
    kill_zombie_chrome_processes()  # Cleanup one last time after quitting driver

    filename = f"4b_winners_wins_{(chunk_num+1)*chunk_size}.parquet"
    pd.DataFrame(output).to_parquet(filename, index=False)
    print(f"✅ Saved chunk {chunk_num+1} → {filename}\n")

# Main loop
total_chunks = (len(fighters) + chunk_size - 1) // chunk_size

for chunk_num in range(total_chunks):
    city = random.choice(VPN_CITIES)
    run_shell("nordvpn disconnect || true")
    run_shell(f"nordvpn connect {city}")

    start_idx = chunk_num * chunk_size
    end_idx = min(start_idx + chunk_size, len(fighters))
    fighter_chunk = fighters.iloc[start_idx:end_idx].to_dict(orient="records")

    try:
        scrape_chunk(fighter_chunk, chunk_num)
    except Exception as e:
        print(f"🚨 Error during chunk {chunk_num+1}: {e}")

    run_shell("nordvpn disconnect")
    time.sleep(10)
