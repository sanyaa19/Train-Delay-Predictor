import pandas as pd
import os

# Step 1: Load Train List
train_list_path = r'C:\Users\sanya\OneDrive\Desktop\TrainScraper\GuwahatiDataset\Train_List.csv'
df_train_list = pd.read_csv(train_list_path)

# Clean the columns to match our naming convention
df_train_list = df_train_list[['Train_Number', 'Train_Name']]  # Keep only relevant columns
df_train_list.columns = ['train_no', 'train_name']  # Rename columns to match
df_train_list['train_no'] = df_train_list['train_no'].astype(str)

# Step 2: Load all route files from Train_Route folder
route_folder = r'C:\Users\sanya\OneDrive\Desktop\TrainScraper\GuwahatiDataset\Train_Route'
all_data = []

for file_name in os.listdir(route_folder):
    if file_name.endswith('.csv'):
        file_path = os.path.join(route_folder, file_name)
        df_route = pd.read_csv(file_path)
        
        # Add train number from filename (e.g., 12423.csv → 12423)
        train_no = file_name.replace('.csv', '')
        df_route['train_no'] = train_no
        df_route['train_no'] = df_route['train_no'].astype(str)
        
        # Select and rename columns to match
        df_route = df_route[['Station', 'Station_Name', 'Average_Delay(min)', 
                             'Right Time (0-15 min\'s)', 'Slight Delay (15-60 min\'s)', 
                             'Significant Delay (>1 Hour)', 'Cancelled/Unknown', 'train_no']]
        
        df_route.columns = ['station', 'station_name', 'average_delay_min', 
                            'right_time_0_15', 'slight_delay_15_60', 'significant_delay_60+', 
                            'cancelled_unknown', 'train_no']
        
        all_data.append(df_route)

# Step 3: Combine all route data
df_all_routes = pd.concat(all_data, ignore_index=True)

# Step 4: Merge with train names
df_final = pd.merge(df_all_routes, df_train_list, on='train_no', how='left')

# Step 5: Save the final dataset to a CSV file
save_path = r'C:\Users\sanya\OneDrive\Desktop\TrainScraper\GuwahatiDataset\guwahati_dataset.csv'
df_final.to_csv(save_path, index=False)

print("✅ guwahati_dataset.csv created successfully!")