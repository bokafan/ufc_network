import pandas as pd
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
import tempfile
import shutil
import os

# Load the input parquet with UFC event URLs
df = pd.read_parquet("2a_ufc_events.parquet")

# Use rows 12 to 16 (first 5 relevant URLs)
urls_to_scrape = df["URL"].tolist()[12:17]

# Setup Chrome options with unique user data directory
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.binary_location = "/usr/bin/google-chrome"  # Path to Chrome binary

# Create a temporary directory for user data
temp_dir = tempfile.mkdtemp()
print(f"Using temporary user data directory: {temp_dir}")
options.add_argument(f"--user-data-dir={temp_dir}")  # ← FIXED: added missing double dash

# Setup Chrome driver
service = Service()  # Will auto-locate chromedriver in PATH
driver = webdriver.Chrome(service=service, options=options)

# Generate headers with fake user agent
ua = UserAgent()
headers = {
    'User-Agent': ua.random
}

rows = []

for url in urls_to_scrape:
    print(f"\nScraping event: {url}")
    try:
        driver.get(url)
        time.sleep(5)  # wait for JS to load
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        winner_divs = soup.select('div.div.hidden.md\:flex.order-1.text-sm.text-tap_3')

        for div in winner_divs:
            link = div.find('a', class_='link-primary-red', href=True)
            if link:
                winner_name = link.get_text(strip=True)
                winner_href = 'https://www.tapology.com' + link['href']
                rows.append({
                    'winner': winner_name,
                    'winner_link': winner_href
                })
                print(f"✓ {winner_name} — {winner_href}")

    except Exception as e:
        print(f"Error scraping {url}: {e}")

    time.sleep(2)  # polite delay

# Quit the driver and clean up temp data
driver.quit()
shutil.rmtree(temp_dir)

# Save to parquet with duplicates removed
results_df = pd.DataFrame(rows).drop_duplicates()
results_df.to_parquet("3a_winners.parquet", index=False)
print("✅ Saved winner data to 3a_winners.parquet")
