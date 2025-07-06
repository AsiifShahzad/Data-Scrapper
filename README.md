# Asia Weather Scraper

This Python script scrapes **historical daily weather data** for major cities across Asian countries using the **Time and Date** website. It collects data for the **past 6 months** and stores it in a structured CSV format.

---

## Features

- Scrapes weather data (temperature, weather description, wind) for **50+ cities** across **30+ Asian countries**
- Collects **hourly data** for each day from the past 6 months
- Stores data in a **CSV file**
- Implements **retry logic**, **logging**, and **progress bars**
- Modular design for scalability

---

## Output

- `asia_weather_data2.csv`: The final CSV file containing all weather records
- `weather_scraper.log`: Log file with scraping events, warnings, and errors

---

## Requirements

Install dependencies using:

```bash
pip install requests beautifulsoup4 pandas tqdm
