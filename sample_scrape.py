import requests
import json
from typing import Optional, Dict, Any

class WNBAScraper:
    """Scraper for WNBA play-by-play statistics"""
    
    BASE_URL = "https://stats.wnba.com/stats/playbyplayv2"
    
    def __init__(self):
        """Initialize the scraper with required headers"""
        self.headers = {
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
    
    def get_play_by_play(self,
                         game_id: str,
                         season: str = "2010-11",
                         season_type: str = "Regular+Season",
                         start_period: int = 1,
                         end_period: int = 10,
                         start_range: int = 0,
                         end_range: int = 55800,
                         range_type: int = 2) -> Optional[Dict[str, Any]]:
        """
        Fetch play-by-play data for a WNBA game
        
        Args:
            game_id: Game ID (e.g., "1021000001")
            season: Season (e.g., "2010-11")
            season_type: Type of season (e.g., "Regular+Season", "Playoffs")
            start_period: Starting period (default: 1)
            end_period: Ending period (default: 10)
            start_range: Start range in seconds (default: 0)
            end_range: End range in seconds (default: 55800)
            range_type: Range type (default: 2)
        
        Returns:
            JSON response as dictionary, or None if request fails
        """
        params = {
            'GameID': game_id,
            'Season': season,
            'SeasonType': season_type,
            'StartPeriod': start_period,
            'EndPeriod': end_period,
            'StartRange': start_range,
            'EndRange': end_range,
            'RangeType': range_type
        }
        
        try:
            response = requests.get(
                self.BASE_URL,
                headers=self.headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
    
    def save_to_file(self, data: Dict[str, Any], filename: str) -> bool:
        """
        Save the scraped data to a JSON file
        
        Args:
            data: Data dictionary to save
            filename: Output filename
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"Data saved to {filename}")
            return True
        except Exception as e:
            print(f"Error saving file: {e}")
            return False


# Example usage
if __name__ == "__main__":
    # Initialize scraper
    scraper = WNBAScraper()
    
    # Fetch play-by-play data for game 1021000001
    print("Fetching WNBA play-by-play data...")
    data = scraper.get_play_by_play(
        game_id="1021000001",
        season="2010-11",
        season_type="Regular+Season"
    )
    
    if data:
        print("Data fetched successfully!")
        print(f"Keys in response: {list(data.keys())}")
        
        # Save to file
        scraper.save_to_file(data, "wnba_playbyplay.json")
        
        # Display some basic info
        if 'resultSets' in data:
            for result_set in data['resultSets']:
                print(f"\nResult Set: {result_set.get('name', 'Unknown')}")
                print(f"Rows: {len(result_set.get('rowSet', []))}")
    else:
        print("Failed to fetch data")