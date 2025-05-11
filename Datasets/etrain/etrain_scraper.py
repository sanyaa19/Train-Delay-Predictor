import requests
import schedule
import time
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor

# Dictionary of train numbers and names
train_info = {
    '12301': 'Rajdhani Express',
    '12302': 'Rajdhani Express',
    '12313': 'Sealdah Rajdhani Express',
    '12314': 'Sealdah Rajdhani Express',
    '12345': 'Saraighat Express',
    '12346': 'Saraighat Express',
    '12377': 'Padatik Express',
    '12378': 'Padatik Express',
    '12367': 'Vikramshila Express',
    '12368': 'Vikramshila Express',
    '12329': 'West Bengal Sampark Kranti Express',
    '12330': 'West Bengal Sampark Kranti Express',
    '12333': 'Vibhuti Express',
    '12334': 'Vibhuti Express',
    '12339': 'Coalfield Express',
    '12340': 'Coalfield Express',
    '12381': 'Poorva Express',
    '12382': 'Poorva Express',
    '12389': 'Gaya–Sealdah Express',
    '12390': 'Sealdah–Gaya Express',
    '12391': 'Shaktipunj Express',
    '12392': 'Shaktipunj Express',
    '12859': 'Gitanjali Express',
    '12860': 'Gitanjali Express',
    '13017': 'Ganadevta Express',
    '13018': 'Ganadevta Express',
    '13053': 'Kulik Express',
    '13054': 'Kulik Express',
    '13173': 'Kanchanjungha Express',
    '13174': 'Kanchanjungha Express',
    '13181': 'Kaziranga Express',
    '13182': 'Kaziranga Express',
    '13465': 'Intercity Express',
    '13466': 'Intercity Express',
    '22897': 'Kandari Express',
    '22898': 'Kandari Express',
    '12307': 'Howrah Jodhpur Express',
    '12308': 'Jodhpur Howrah Express',
    '12857': 'Tamralipta Express',
    '12858': 'Tamralipta Express',
    '13011': 'Malda Town Intercity Express',
    '13012': 'Malda Town Intercity Express',
    '13027': 'Kaviguru Express',
    '13028': 'Kaviguru Express',
    '13147': 'Uttar Banga Express',
    '13148': 'Uttar Banga Express',
    '13149': 'Kanchan Kanya Express',
    '13150': 'Kanchan Kanya Express',
    '13465': 'Intercity Express',
    '13466': 'Intercity Express',
    '12875': 'Howrah Bhubaneswar Superfast Express',
    '12876': 'Bhubaneswar Howrah Superfast Express',
}

def extract_station_data(soup):
    rows = soup.find_all('a', class_='runStatStn blocklink rnd5')
    result = []

    for row in rows:
        station = row.find('div').text.strip()
        avg_delay_div = row.find('div', class_='inlineblock pdl5')
        delay_min = None
        if avg_delay_div:
            delay_text = avg_delay_div.text.strip()
            if "Min" in delay_text:
                try:
                    delay_min = int(delay_text.split()[2])  # e.g., "Avg. Delay: 1 Min's"
                except:
                    delay_min = None
        result.append((station, delay_min))
    return result

def scrape_data_for_train(train_no, train_name, date_str):
    url = f"https://etrain.info/train/{train_name.replace(' ', '-')}-{train_no}/history"
    headers = {"User-Agent": "Mozilla/5.0"}
    all_data = []

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {url}, status: {response.status_code}")
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        station_data = extract_station_data(soup)

        if not station_data:
            print(f"❌ No data found for {train_no}")
            return []

        for station, delay_min in station_data:
            all_data.append({
                'date': date_str,
                'train_no': train_no,
                'train_name': train_name,
                'station': station,
                'delay_min': delay_min,
                'status': "Unknown"
            })

    except Exception as e:
        print(f"🚨 Error while processing {train_no}: {e}")
    
    return all_data

def scrape_data():
    today = datetime.today()
    date_str = today.strftime('%Y-%m-%d')
    filename_date = today.strftime('%Y%m%d')
    all_data = []

    with ThreadPoolExecutor() as executor:
        # Create a list of futures for each train
        futures = [executor.submit(scrape_data_for_train, train_no, train_name, date_str) 
                   for train_no, train_name in train_info.items()]

        # Collect results from all the futures
        for future in futures:
            data = future.result()
            all_data.extend(data)

    # Convert to DataFrame and write to CSV
    if all_data:
        df = pd.DataFrame(all_data)
        save_path = r'C:\Users\sanya\OneDrive\Desktop\Train-Delay-Predictor\Datasets\etrain\daily_raw.csv'

        if os.path.exists(save_path):
            df.to_csv(save_path, mode='a', index=False, header=False)
            print("📦 Appended to daily_raw.csv")
        else:
            df.to_csv(save_path, index=False)
            print("✅ Created new daily_raw.csv")
    else:
        print("⚠️ No data scraped.")

# Manual run
def run_scraper():
    print("Running the scraper manually...")
    scrape_data()

# First run scraper manually
run_scraper()

# Schedule the scraper to run daily at 9:00 AM
schedule.every().day.at("09:00").do(scrape_data)

# Start the scheduler
print("Scheduler started. Waiting for scheduled task...")

# Keep the script running to continue checking the schedule
while True:
    schedule.run_pending()
    time.sleep(1)
