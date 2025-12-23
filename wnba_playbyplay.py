import pandas as pd
import requests
import os
import time

def scrape_pbp_data():
    # 1. Setup Input/Output
    input_file = 'data/wnba_game_dates.csv'
    output_dir = 'pbp_data'
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found.")
        return
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df = pd.read_csv(input_file)

    # 2. Filter out Preseason and All-Star
    # Using the specific strings found in your CSV
    valid_types = ['Regular Season', 'Playoffs']
    df_filtered = df[df['seasonType'].isin(valid_types)].copy()

    # 3. Configure Headers from your working sample
    headers = {
        'authority': 'stats.wnba.com',
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'origin': 'https://stats.wnba.com',
        'referer': 'https://stats.wnba.com/game/',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36',
        'x-nba-stats-origin': 'stats',
        'x-nba-stats-token': 'true'
    }

    session = requests.Session()
    session.headers.update(headers)

    print(f"Starting scrape for {len(df_filtered)} games...")

    for index, row in df_filtered.iterrows():
        # Format GameID (ensure it's 10 digits)
        game_id = str(row['gameId']).zfill(10)
        save_path = os.path.join(output_dir, f"{game_id}.csv")

        # --- Avoid Rescraping ---
        if os.path.exists(save_path):
            continue

        # Format Season: '2010' -> '2010-11'
        year_val = int(str(row['date'])[:4])
        season_param = f"{year_val}-{str(year_val + 1)[2:]}"
        
        # Format SeasonType: 'Regular Season' -> 'Regular+Season'
        season_type_param = row['seasonType'].replace(" ", "+")

        # Parameters for the API
        params = {
            'GameID': game_id,
            'Season': season_param,
            'SeasonType': season_type_param,
            'StartPeriod': 1,
            'EndPeriod': 10,
            'StartRange': 0,
            'EndRange': 55800,
            'RangeType': 2
        }

        try:
            url = "https://stats.wnba.com/stats/playbyplayv2"
            response = session.get(url, params=params, timeout=20)
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract headers and rows from the JSON
                if 'resultSets' in data and len(data['resultSets']) > 0:
                    result_set = data['resultSets'][0]
                    pbp_df = pd.DataFrame(result_set['rowSet'], columns=result_set['headers'])
                    
                    # Save to individual CSV
                    pbp_df.to_csv(save_path, index=False)
                    print(f"Successfully saved {game_id} ({row['homeTeam']} vs {row['awayTeam']})")
                else:
                    print(f"Empty result set for GameID {game_id}")
            else:
                print(f"Failed GameID {game_id} | Status: {response.status_code}")

            # Standard delay to avoid rate limiting
            time.sleep(2.0)

        except Exception as e:
            print(f"Request error on {game_id}: {e}")
            time.sleep(5)

    print("Scrape complete.")

if __name__ == "__main__":
    scrape_pbp_data()