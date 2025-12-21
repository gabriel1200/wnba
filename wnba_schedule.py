import requests
import pandas as pd
import time

def scrape_wnba_schedules():
    all_games_data = []
    
    # Headers are necessary because the WNBA/NBA APIs often block generic scripts
  
    headers = {
        "Host": "www.wnba.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        'Referer': 'https://www.wnba.com/schedule',
        'Origin': 'https://www.wnba.com',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
    # Iterate through seasons from 2010 to 2024
    for year in range(2010, 2025):
        print(f"Fetching season: {year}...")
        url = f"https://www.wnba.com/api/schedule?season={year}&regionId=1"
        
        try:
            response = requests.get(url, headers=headers)
        
            response.raise_for_status() # Check for HTTP errors
            data = response.json()
            
            # The structure based on WNBA API standard: 
            # leagueSchedule -> gameDates -> games (list)
            game_dates = data.get('leagueSchedule', {}).get('gameDates', [])
            
            for date_entry in game_dates:
                games = date_entry.get('games', [])
                for game in games:
                    # Extracting fields as per your screenshot
                    game_info = {
                        'gameId': game.get('gameId'),
                        'homeTeam': game.get('homeTeam', {}).get('teamName'),
                        'awayTeam': game.get('awayTeam', {}).get('teamName'),
                        'date': game.get('gameDateEst'), # Standard date format in the API
                        'seasonType': game.get('seasonType')
                    }
                    all_games_data.append(game_info)
            
            # Small pause to avoid rate limiting
            time.sleep(1)
            
        except Exception as e:
            print(f"Could not retrieve data for {year}: {e}")

    # Create DataFrame and Save to CSV
    if all_games_data:
        df = pd.DataFrame(all_games_data)
        
        # Clean up date if needed (stripping time if you only want the date)
        # df['date'] = pd.to_datetime(df['date']).dt.date
        
        df.to_csv('wnba_game_dates.csv', index=False)
        print(f"Successfully saved {len(df)} games to wnba_game_dates.csv")
    else:
        print("No data collected.")

if __name__ == "__main__":
    scrape_wnba_schedules()