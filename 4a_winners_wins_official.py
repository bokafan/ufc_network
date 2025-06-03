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

# Load fighter URL data
df = pd.read_parquet("4c_unique_fighter_urls.parquet")
fighters = df[['fighter_name', 'fighter_url']].drop_duplicates().reset_index(drop=True)
chunk_size = 50

def run_shell(cmd):
    print(f"\n▶️ Running: {cmd}")
    subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def clean_text(text):
    if isinstance(text, str):
        return text.encode('utf-8', 'ignore').decode('utf-8')
    return text

def scrape_chunk(fighter_chunk, chunk_num):
    output = []

    for row in fighter_chunk:
        fighter_name = row['fighter_name']
        fighter_url = urljoin("https://www.tapology.com", row['fighter_url'])

        print(f"Scraping {fighter_name}: {fighter_url}")
        row_count_before = len(output)

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        driver = webdriver.Chrome(options=chrome_options)

        try:
            try:
                driver.get(fighter_url)
            except Exception as e:
                print(f"Failed initial load: {e}. Switching VPN and retrying...")
                driver.quit()
                run_shell("nordvpn disconnect || true")
                time.sleep(3)
                new_city = random.choice(VPN_CITIES)
                run_shell(f"nordvpn connect {new_city}")
                time.sleep(5)

                driver = webdriver.Chrome(options=chrome_options)
                try:
                    driver.get(fighter_url)
                except Exception as e2:
                    print(f"Retry after VPN switch also failed: {e2}")
                    driver.quit()
                    continue

            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "section.fighterFightResults"))
            )
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # Initialize defaults
            fighter_dob = fighter_height = fighter_reach = foundation_style = 'null'

            # Parse fighter bio data
            details_container = soup.select_one('#standardDetails')
            if details_container:
                for div in details_container.select('div'):
                    text = div.get_text(separator=" ", strip=True)

                    # DOB
                    dob_match = re.search(r"Date of Birth:\s*([\d]{4} [A-Za-z]{3} \d{1,2})", text)
                    if dob_match:
                        fighter_dob = dob_match.group(1)

                    #Foundation Style
                    style_match = re.search(r"Foundation Style:\s*([^\n]+)", text)
                    if style_match:
                        foundation_style = style_match.group(1).strip()
                
                    # Height and Reach
                    if "Height:" in text and "Reach:" in text:
                        height_match = re.search(r"Height:\s*([^|]+)", text)
                        reach_match = re.search(r"Reach:\s*([^\s]+)", text)
                        if height_match:
                            fighter_height = height_match.group(1).strip()
                        if reach_match:
                            fighter_reach = reach_match.group(1).strip()

            results_container = soup.select_one("section.fighterFightResults div#proResults")

            if not results_container:
                print("❌  No <div id='proResults'> found — skipping.")
                driver.quit()
                continue

            bouts = results_container.select("div[data-fighter-bout-target='bout']")
            print(f"✅ Found {len(bouts)} bouts")

            for bout in bouts:
                if 'Amateur Bouts' in bout.text:
                    print("🚫 Reached 'Amateur Bouts', stopping scrape for this fighter.")
                    break

                result_div = bout.select_one("div.result div")
                result_text = result_div.text.strip() if result_div else ''
                if result_text not in ['W', 'L']:
                    continue

                record_span = bout.select_one("span[title='Fighter Record Before Fight']")
                if not record_span or not re.match(r"^\d+-\d+", record_span.text.strip()):
                    continue

                opponent_tag = bout.select_one('a[href*="/fighters/"]')
                opponent_name = opponent_tag.text.strip() if opponent_tag else 'null'
                opponent_link = urljoin("https://www.tapology.com", opponent_tag['href']) if opponent_tag else 'null'

                event_tag = bout.select_one('a[href*="/fightcenter/events/"]')
                event_url = urljoin("https://www.tapology.com", event_tag['href']) if event_tag else 'null'

                event_year = event_md = 'null'
                date_container = bout.select_one('div.flex.flex-col.justify-around.items-center')

                if date_container:
                    spans = date_container.find_all('span')
                    if len(spans) >= 2:
                        event_year = spans[0].text.strip()
                        event_md = spans[1].text.strip()

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
                    'original_fighter_name': clean_text(fighter_name),
                    'original_fighter_url': clean_text(fighter_url),
                    'fighter_dob': clean_text(fighter_dob),
                    'fighter_height': clean_text(fighter_height),
                    'fighter_reach': clean_text(fighter_reach),
                    'foundational_style': clean_text(foundation_style),
                    'fighter_reach': clean_text(fighter_reach),
                    'opponent_name': clean_text(opponent_name),
                    'opponent_url': clean_text(opponent_link),
                    'event_url': clean_text(event_url),
                    'event_year': clean_text(event_year),
                    'event_month_day': clean_text(event_md),
                    'duration': clean_text(duration),
                    'weight': clean_text(weight),
                    'odds': clean_text(odds),
                    'victory_details': clean_text(victory_details),
                    'result': clean_text(result_text)
                })

            new_rows = len(output) - row_count_before
            if new_rows > 0:
                print(f"📦 Added {new_rows} row(s) for {fighter_name}")
            else:
                print(f"⚠️ No valid rows scraped for {fighter_name}")

        except Exception as e:
            print(f"❌ Error scraping {fighter_url}: {e}")
        finally:
            driver.quit()

        time.sleep(random.uniform(2, 8))

    filename = f"5a_final_historical_fighter_bouts_final_{(chunk_num+1)*chunk_size}.parquet"
    pd.DataFrame(output).to_parquet(filename, index=False)
    print(f"✅ Saved chunk {chunk_num+1} → {filename}\n")

# Loop through chunks
total_chunks = (len(fighters) + chunk_size - 1) // chunk_size

for chunk_num in range(242, total_chunks):
    city = random.choice(VPN_CITIES)
    run_shell("nordvpn disconnect || true")
    run_shell(f"nordvpn connect {city}")

    start_idx = chunk_num * chunk_size
    end_idx = start_idx + chunk_size
    fighter_chunk = fighters.iloc[start_idx:end_idx].to_dict(orient="records")

    try:
        scrape_chunk(fighter_chunk, chunk_num)
    except Exception as e:
        print(f"🚨 Error during chunk {chunk_num+1}: {e}")

    run_shell("nordvpn disconnect")
    time.sleep(10)
