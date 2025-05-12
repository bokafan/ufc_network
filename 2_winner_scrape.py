import pandas as pd
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from fake_useragent import UserAgent

# Load the input parquet with UFC event URLs
df = pd.read_parquet("2a_ufc_events.parquet")

# Use rows 12 to 16 (first 5 relevant URLs)
urls_to_scrape = df["URL"].tolist()[12:17]

# Setup Chrome driver
service = Service()
options = webdriver.ChromeOptions()
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

# Quit the driver
driver.quit()

# Save to parquet with duplicates removed
results_df = pd.DataFrame(rows).drop_duplicates()
results_df.to_parquet("3a_winners.parquet", index=False)
print("✅ Saved winner data to 3a_winners.parquet")
