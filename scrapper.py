import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from datetime import datetime, timedelta
import time
import random
from tqdm import tqdm
import re

# --- Logging Config ---
logging.basicConfig(
    filename='testlog.log',
    filemode='a',  # append logs
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Browsing Headers ---
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/114.0.0.0 Safari/537.36"),
    "Accept": ("text/html,application/xhtml+xml,"
               "application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.timeanddate.com/",
    "Connection": "keep-alive"
}

# --- City Info ---
CITIES = {
    "Russia": ["Moscow", "Novosibirsk", "Yekaterinburg"],
    "China": ["Beijing", "Shanghai", "Chengdu", "Ürümqi"],
    "Pakistan": ['Lahore', 'Karachi', 'Islamabad'],
    "India": ["New Delhi", "Mumbai", "Kolkata", "Chennai"],
    "Kazakhstan": ["Almaty", "Karaganda", "Aktobe"],
    "Saudi Arabia": ["Riyadh", "Jeddah", "Dammam", "Abha"],
    "Iran": ["Tehran", "Mashhad", "Isfahan", "Shiraz"],
    "Japan": ["Tokyo", "Osaka", "Sapporo"],
    "Vietnam": ["Hanoi"],
    "Malaysia": ["Kuala Lumpur", "George Town", "Kota Kinabalu"],
    "Philippines": ["Manila", "Cebu City", "Davao"],
    "Oman": ["Muscat", "Salalah", "Nizwa"],
    "Thailand": ["Bangkok", "Chiang Mai", "Phuket"],
    "Bangladesh": ["Dhaka", "Chittagong"],
    "Nepal": ["Kathmandu", "Pokhara"],
    "Sri Lanka": ["Colombo", "Kandy"],
    "Bhutan": ["Thimphu", "Phuntsholing"],
    "Singapore": ["Singapore"],
    "Bahrain": ["Manama"],
    "Maldives": ["Malé"],
    "Afghanistan": ["Kabul"],
    "Azerbaijan": ["Baku"],
    "Iraq": ["Baghdad"],
    "Kuwait": ["Kuwait City"],
    "North Korea": ["Pyongyang"],
    "Palestine": ["Gaza City"],
    "Qatar": ["Doha"],
    "South Korea": ["Seoul"],
    "Tajikistan": ["Dushanbe"],
    "Turkey": ["Istanbul"],
    "United Arab Emirates": ["Abu Dhabi", "Dubai"],
    "Uzbekistan": ["Samarkand"],
    "Yemen": ["Aden"]
}

BASE_URL = "https://www.timeanddate.com/weather"

# --- Helper Functions ---
def build_url(country, city, date):
    return (f"{BASE_URL}/"
            f"{country.lower().replace(' ', '-')}/"
            f"{city.lower().replace(' ', '-')}/"
            f"historic?hd={date.strftime('%Y%m%d')}")

def fetch_page(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                return r.text
            else:
                logging.warning(f"Non-200 response: {r.status_code} for URL: {url}")
                break
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
            time.sleep(2)
    return None

def parse_weather_data(html, city, country, date):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'id': 'wt-his'})
    if not table:
        logging.warning(f"No weather table for {city}, {country} on {date}")
        return []

    weather_data = []
    for row in table.find_all('tr')[1:]:
        th = row.find('th')
        cols = row.find_all('td')
        if not th or len(cols) < 3:
            continue

        raw_time = th.text.strip()
        match = re.match(r'^(\d{1,2}:\d{2})', raw_time)
        if not match:
            continue

        time_str = datetime.strptime(match.group(1), "%H:%M").strftime("%H:%M")

        weather_data.append({
            'Country': country,
            'City': city,
            'Date': date.strftime('%Y-%m-%d'),
            'Time': time_str,
            'Temperature_C': cols[0].text.strip().replace("°C", ""),
            'Weather': cols[1].text.strip(),
            'Wind': cols[2].text.strip()
        })

    return weather_data

def save_to_csv(data, filename='weatherdata.csv'):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, encoding='utf-8')
    logging.info(f"Data saved to {filename}: {len(df)} records total")

# --- Main Scraper ---
def scrape_weather():
    all_weather = []
    today = datetime.now()
    start_date = today - timedelta(days=180)

    for country, cities in CITIES.items():
        for city in cities:
            for single_date in tqdm(
                [start_date + timedelta(days=i) for i in range(180)],
                desc=f"{city}, {country}"
            ):
                url = build_url(country, city, single_date)
                html = fetch_page(url)
                if html:
                    daily = parse_weather_data(html, city, country, single_date)
                    if daily:
                        all_weather.extend(daily)
                        logging.info(f"Scraped {len(daily)} records for {city}, {country} on {single_date.date()}")
                time.sleep(random.uniform(1.5, 3.0))

    save_to_csv(all_weather)

if __name__ == "__main__":
    scrape_weather()
