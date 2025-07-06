import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from datetime import datetime, timedelta
import time
from tqdm import tqdm

# -------------------- Logging Config --------------------
logging.basicConfig(
    filename='weather_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# -------------------- City Info --------------------

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
    'Pakistan': ['Lahore', 'Karachi', 'Islamabad'],
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

# -------------------- Helper Functions --------------------

def build_url(country, city, date):
    """
    Constructs the URL for the historical weather data.
    """
    country_formatted = country.lower().replace(" ", "-")
    city_formatted = city.lower().replace(" ", "-")
    return f"{BASE_URL}/{country_formatted}/{city_formatted}/historic?hd={date.strftime('%Y%m%d')}"

def fetch_page(url, retries=3):
    """
    Fetches a page with retries and error handling.
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.text
            else:
                logging.warning(f"Non-200 response: {response.status_code} for URL: {url}")
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
            time.sleep(2)
    return None

def parse_weather_data(html, city, country, date):
    """
    Parses hourly weather data from HTML.
    """
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'id': 'wt-his'})

    if not table:
        logging.warning(f"No weather table found for {city}, {country} on {date}")
        return []

    rows = table.find_all('tr')
    weather_data = []

    for row in rows[1:]:
        cols = row.find_all('td')
        if len(cols) >= 4:
            time_of_day = cols[0].text.strip()
            temp = cols[1].text.strip().replace("°C", "")
            weather = cols[2].text.strip()
            wind = cols[3].text.strip()

            weather_data.append({
                'Country': country,
                'City': city,
                'Date': date.strftime('%Y-%m-%d'),
                'Time': time_of_day,
                'Temperature_C': temp,
                'Weather': weather,
                'Wind': wind
            })

    return weather_data

def save_to_csv(data, filename='asia_weather_data2.csv'):
    """
    Saves the weather data to CSV.
    """
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    logging.info(f"Data saved to {filename}")

# -------------------- Main Scraper --------------------

def scrape_weather():
    """
    Orchestrates the scraping process.
    """
    all_weather = []
    today = datetime.now()
    start_date = today - timedelta(days=180)  # Past 6 months

    for country, cities in CITIES.items():
        for city in cities:
            for single_date in tqdm([start_date + timedelta(days=n) for n in range(0, 180)], desc=f"Scraping {city}, {country}"):
                try:
                    url = build_url(country, city, single_date)
                    html = fetch_page(url)
                    if html:
                        daily_data = parse_weather_data(html, city, country, single_date)
                        all_weather.extend(daily_data)
                    time.sleep(1)  # Be polite to the server
                except Exception as e:
                    logging.error(f"Failed to process {city}, {country} on {single_date}: {e}")
                    continue

    save_to_csv(all_weather)

# -------------------- Entry Point --------------------

if __name__ == "__main__":
    scrape_weather()
