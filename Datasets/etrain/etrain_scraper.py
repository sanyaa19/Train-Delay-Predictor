import schedule
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os

def scrape_data():
    # Setup headless Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)

    # Dictionary of train numbers and their corresponding names
    train_info = {
  "12303": "POORVA EXPRESS",
  "12304": "POORVA EXPRESS",
  "12305": "RAJDHANI EXPRESS",
  "12306": "RAJDHANI EXPRESS",
  "12311": "NETAJI EXPRESS",
  "12312": "NETAJI EXPRESS",
  "12313": "RAJDHANI EXPRESS",
  "12314": "RAJDHANI EXPRESS",
  "12317": "AKAL TAKHT EXP",
  "12318": "AKAL TAKHT EXP",
  "12333": "VIBHUTI EXPRESS",
  "12334": "VIBHUTI EXPRESS",
  "12351": "HWH RJPB EXP",
  "12352": "RJPB HWH EXP",
  "12359": "PNBE GARIB RATH",
  "12360": "KOL GARIB RATH",
  "12369": "KUMBHA EXPRESS",
  "12370": "KUMBHA EXPRESS",
  "12377": "PADATIK EXPRESS",
  "12378": "PADATIK EXPRESS",
  "13005": "HWH ASR MAIL",
  "13006": "ASR HWH MAIL",
  "13019": "BAGH EXPRESS",
  "13020": "BAGH EXPRESS",
  "13021": "MITHILA EXPRESS",
  "13022": "MITHILA EXPRESS",
  "13105": "SDAH BUI EXPRES",
  "13106": "BUI SDAH EXPRES",
  "13123": "SEALDAH SMI EXP",
  "13124": "SMI SEALDAH EXP",
  "13137": "KOAA AMH EXPRES",
  "13138": "AMH KOAA EXPRES",
  "13173": "KANCHANJUNGA EXP",
  "13174": "KANCHANJUNGA EXP",
  "13175": "KANCHANJUNGA EXP",
  "13176": "KANCHANJUNGA EXP",
  "13185": "GANGA SAGAR EXP",
  "13186": "GANGA SAGAR EXP",
  "15047": "PURBANCHAL EXP",
  "15048": "PURBANCHAL EXP",
  "15049": "PURBANCHAL EXP",
  "15050": "PURBANCHAL EXP",
  "15271": "JANSADHARAN EXP",
  "15272": "JANSADHARAN EXP",
  "15653": "AMARNATH EXPRESS",
  "15654": "AMARNATH EXPRESS",
  "18047": "AMARAVATI EXP",
  "18048": "AMARAVATI EXP",
  "22197": "KOAA VGLB SF EX",
  "22198": "VGLB KOAA SF EX",
  "22213": "PNBE DURONTO EX",
  "22214": "DURONTO EXPRESS",
  "22303": "VANDE BHARAT EXP",
  "22304": "VANDE BHARAT EXP",
  "22347": "VANDE BHARAT EXP",
  "22348": "VANDE BHARAT EXP",
  "22877": "ANTYODAYA EXP",
  "22878": "ANTYODAYA EXP"
}


    # Today's date
    today = datetime.today()
    date_str = today.strftime('%Y-%m-%d')
    filename_date = today.strftime('%Y%m%d')  # For naming file like daily_raw_20250510.csv
    all_data = []

    # Loop over each train number and its name
    for train_no, train_name in train_info.items():
        print(f"Scraping data for train number: {train_no} ({train_name})")

        # Construct the URL dynamically based on train number and train name
        url = f"https://etrain.info/train/{train_name.replace(' ', '-')}-{train_no}/history"
        driver.get(url)

        # Wait for the page to load
        time.sleep(5)

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Find all divs that contain station information and delay
        rows = soup.find_all('a', class_='runStatStn blocklink rnd5')

        if not rows:
            print(f"No rows found for train {train_name} ({train_no}).")
        else:
            print(f"Found {len(rows)} rows for train {train_name} ({train_no}).")

        # Loop through each row and extract relevant data
        for row in rows:
            # Extract station name
            station = row.find('div').text.strip()

            # Extract average delay
            avg_delay_div = row.find('div', class_='inlineblock pdl5')
            if avg_delay_div:
                delay_text = avg_delay_div.text.strip()  # Example: "Avg. Delay: 1 Min's"
                delay_min = None
                if "Min" in delay_text:
                    delay_min = int(delay_text.split()[2])  # Extracts '1' from "Avg. Delay: 1 Min's"
            else:
                delay_min = None  # In case there's no delay info

            # Assuming status is not present directly here, so setting it as empty for now
            status = "Unknown"  # Adjust this as needed

            # Append the data for this row with the train_name column
            all_data.append({
                'date': date_str,
                'train_no': train_no,
                'train_name': train_name,  # Added train_name column
                'station': station,
                'delay_min': delay_min,
                'status': status
            })

    # Quit the driver after scraping
    driver.quit()

    # Convert to DataFrame
    df = pd.DataFrame(all_data)

    # Check if data is being scraped properly
    if df.empty:
        print("No data scraped. Please check the scraping process.")
    else:
        print(f"Scraped {len(df)} records.")

    # Append to daily_raw.csv (long-term)
    append_path = r'C:\Users\sanya\OneDrive\Desktop\Train-Delay-Predictor\Datasets\etrain\daily_raw.csv'

    # Check if the file exists, if so, append without writing the header again
    if os.path.exists(append_path):
        df.to_csv(append_path, mode='a', index=False, header=False)
        print("📦 Appended new data to daily_raw.csv")
    else:
        df.to_csv(append_path, index=False)
        print("✅ Created new daily_raw.csv and added data")

# Function to run the scraper immediately (for manual execution)
def run_scraper():
    print("Manually running the scraper...")
    scrape_data()

# Schedule the scraper to run daily at 9:00 AM
schedule.every().day.at("09:00").do(scrape_data)

# Start the scheduler
print("Scheduler started. Waiting for scheduled task...")

# Run the scraper immediately if you want to test or run manually
run_scraper()

# Keep the script running to continue checking the schedule
while True:
    schedule.run_pending()
    time.sleep(1)
