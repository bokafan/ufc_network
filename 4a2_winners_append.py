import os
import random
import time
import pandas as pd
import subprocess
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

# Load rescrape fighter URL data
df = pd.read_parquet("rescrape_urls.parquet")

# Rename column to match expected format
df = df.rename(columns={"original_fighter_url": "fighter_url"})

# Add dummy fighter_name (optional, if required elsewhere in your code)
df["fighter_name"] = "Unknown"

# Drop duplicates and reset index
fighters = df[["fighter_name", "fighter_url"]].drop_duplicates().reset_index(drop=True)

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

            results_container = soup.select_one("section.fighterFightResults div#proResults")
            if not results_container:
                print("❌  No <div id='proResults'> found — skipping.")
                driver.quit()
                continue

            bouts = results_container.select("div[data-fighter-bout-target='bout']")
            print(f"✅ Found {len(bouts)} bouts")

            for bout in bouts:
                if 'Amateur Bouts' in bout.text:
                    break

                result_div = bout.select_one("div.result div")
                result_text = result_div.text.strip() if result_div else ''
                if result_text not in ['W', 'L']:
                    continue

                record_span = bout.select_one("span[title='Fighter Record Before Fight']")
                if not record_span:
                    continue

                opponent_tag = bout.select_one('a[href*="/fighters/"]')
                opponent_url = urljoin("https://www.tapology.com", opponent_tag['href']) if opponent_tag else 'null'

                date_container = bout.select_one('div.flex.flex-col.justify-around.items-center')
                event_year = event_md = 'null'
                if date_container:
                    spans = date_container.find_all('span')
                    if len(spans) >= 2:
                        event_year = spans[0].text.strip()
                        event_md = spans[1].text.strip()

                finish_shortened = 'null'
                short_finish_div = bout.select_one("div.-rotate-90")
                if short_finish_div:
                    finish_shortened = short_finish_div.text.strip()

                output.append({
                    'original_fighter_url': clean_text(fighter_url),
                    'opponent_url': clean_text(opponent_url),
                    'event_year': clean_text(event_year),
                    'event_month_day': clean_text(event_md),
                    'result': clean_text(result_text),
                    'finish_shortened': clean_text(finish_shortened)
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

    filename = f"5a_rescrape_output_{(chunk_num+1)*chunk_size}.parquet"
    pd.DataFrame(output).to_parquet(filename, index=False)
    print(f"✅ Saved chunk {chunk_num+1} → {filename}\n")

# Loop through chunks
total_chunks = (len(fighters) + chunk_size - 1) // chunk_size

for chunk_num in range(total_chunks):
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
