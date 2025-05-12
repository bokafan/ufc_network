import pandas as pd
import time
import re
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from fake_useragent import UserAgent

# Setup Chrome driver
service = Service()
options = webdriver.ChromeOptions()
driver = webdriver.Chrome(service=service, options=options)

# Target URL
url = 'https://www.tapology.com/fightcenter/promotions/1-ultimate-fighting-championship-ufc'

# Initialize output list
all_links = []

# Generate headers with fake user agent
ua = UserAgent()
headers = {
    'User-Agent': ua.random
}

# Get the initial page
response = requests.get(url, headers=headers)
time.sleep(5)

if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')

    # Get total number of pages
    span_last = soup.find('span', class_='last')
    if span_last:
        a_tag = span_last.find('a')
        if a_tag and 'href' in a_tag.attrs:
            last_page_href = a_tag['href']
            match = re.search(r'page=(\d+)', last_page_href)
            max_page = int(match.group(1)) if match else 1
        else:
            max_page = 1
    else:
        max_page = 1

    # Generate all subpage URLs
    all_subpages = [f"{url}?page={i}" for i in range(1, max_page + 1)]

    for subpage in all_subpages:
        try:
            driver.get(subpage)
            time.sleep(15)  # Allow JS to render fully
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')

            event_links = soup.select('span.hidden.md\\:inline.text-tap_3 a')
            print(f"Processing page {subpage} - found {len(event_links)} links")

            for a_tag in event_links:
                if a_tag and 'href' in a_tag.attrs:
                    full_url = 'https://www.tapology.com' + a_tag['href']
                    all_links.append(full_url)

        except Exception as e:
            print(f"Error processing {subpage}: {e}")

else:
    print(f"Failed to fetch base URL: status {response.status_code}")

# Quit the driver
driver.quit()

# Save results to a Parquet file
df = pd.DataFrame({'URL': all_links})
df.to_parquet("scraped_urls.parquet", index=False)

print(f"Saved {len(all_links)} URLs to scraped_urls.parquet")
