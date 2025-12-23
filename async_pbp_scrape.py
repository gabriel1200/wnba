import pandas as pd
import requests
import os
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

def scrape_pbp_data(max_workers=5):
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
    valid_types = ['Regular Season', 'Playoffs']
    df_filtered = df[df['seasonType'].isin(valid_types)].copy()

    # 3. Pre-filter already scraped games
    games_to_scrape = []
    for index, row in df_filtered.iterrows():
        game_id = str(row['gameId']).zfill(10)
        save_path = os.path.join(output_dir, f"{game_id}.csv")
        if not os.path.exists(save_path):
            games_to_scrape.append(row)
    
    if not games_to_scrape:
        print("All games already scraped!")
        return
    
    print(f"Found {len(df_filtered)} total games, {len(games_to_scrape)} need scraping...")

    # 4. Header randomization pools
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    ]
    
    platforms = ['"Windows"', '"macOS"', '"Linux"']
    chrome_versions = ['121', '122', '123']
    
    def get_random_headers():
        ua = random.choice(user_agents)
        platform = random.choice(platforms)
        version = random.choice(chrome_versions)
        
        # Determine if mobile based on UA
        is_mobile = 'Mobile' in ua or 'Android' in ua
        
        return {
            'authority': 'stats.wnba.com',
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': random.choice([
                'en-US,en;q=0.9',
                'en-US,en;q=0.9,es;q=0.8',
                'en-GB,en;q=0.9,en-US;q=0.8',
            ]),
            'origin': 'https://stats.wnba.com',
            'referer': 'https://stats.wnba.com/game/',
            'sec-ch-ua': f'"Chromium";v="{version}", "Google Chrome";v="{version}", "Not?A_Brand";v="24"',
            'sec-ch-ua-mobile': '?1' if is_mobile else '?0',
            'sec-ch-ua-platform': platform,
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': ua,
            'x-nba-stats-origin': 'stats',
            'x-nba-stats-token': 'true'
        }

    # Thread-safe printing
    print_lock = Lock()
    
    def scrape_single_game(row):
        # Create session with randomized headers per thread
        session = requests.Session()
        session.headers.update(get_random_headers())
        
        game_id = str(row['gameId']).zfill(10)
        save_path = os.path.join(output_dir, f"{game_id}.csv")

        # Format Season and SeasonType
        year_val = int(str(row['date'])[:4])
        season_param = f"{year_val}-{str(year_val + 1)[2:]}"
        season_type_param = row['seasonType'].replace(" ", "+")

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
                
                if 'resultSets' in data and len(data['resultSets']) > 0:
                    result_set = data['resultSets'][0]
                    pbp_df = pd.DataFrame(result_set['rowSet'], columns=result_set['headers'])
                    pbp_df.to_csv(save_path, index=False)
                    
                    with print_lock:
                        print(f"✓ Saved {game_id} ({row['homeTeam']} vs {row['awayTeam']})")
                    return True
                else:
                    with print_lock:
                        print(f"✗ Empty result for {game_id}")
                    return False
            else:
                with print_lock:
                    print(f"✗ Failed {game_id} | Status: {response.status_code}")
                return False

        except Exception as e:
            with print_lock:
                print(f"✗ Error on {game_id}: {e}")
            return False
        finally:
            session.close()

    # 5. Parallel execution
    success_count = 0
    fail_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(scrape_single_game, row): row for row in games_to_scrape}
        
        for future in as_completed(futures):
            if future.result():
                success_count += 1
            else:
                fail_count += 1
            
            # Random delay between completions to appear more organic
            time.sleep(random.uniform(0.3, 0.7))

    print(f"\nScrape complete! Success: {success_count}, Failed: {fail_count}")

if __name__ == "__main__":
    # Adjust max_workers based on your needs (5-10 is usually safe)
    scrape_pbp_data(max_workers=1)