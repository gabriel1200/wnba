# Define the API URL

import pandas as pd
import requests
import sys
import os
import time
from datetime import datetime

url = "https://api.pbpstats.com/get-totals/wnba"

# Get the current year
current_year = datetime.now().year

# Iterate over seasons from 2001 to current year

def fetch_wnba_data(start_year, end_year, season_type='rs', data_type='Player', save_to_csv=True):
    """
    Fetch WNBA player or team stats from the PBP Stats API for a given range of seasons and season type.

    Parameters:
    - start_year (int): The starting year (e.g., 2001).
    - end_year (int): The ending year (inclusive, e.g., 2024).
    - season_type (str): Season type, 'rs' for Regular Season or 'ps' for Playoffs.
    - data_type (str): Data type, 'Player' or 'Team'.
    - save_to_csv (bool): Whether to save the data as CSV files. Default is True.

    Returns:
    - List of DataFrames containing the fetched data for each season.
    """
    # Define the API URL
    url = "https://api.pbpstats.com/get-totals/wnba"

    # Map season type input to API-compatible parameter
    season_type_map = {'rs': "Regular Season", 'ps': "Playoffs"}
    if season_type not in season_type_map:
        raise ValueError("Invalid season type. Use 'rs' for Regular Season or 'ps' for Playoffs.")

    # Converted season type
    season_type_label = season_type_map[season_type]
    all_data = []  # Store dataframes for return

    for year in range(start_year, end_year + 1):
        # Format the season for API (e.g., "2024-25")
        season = f"{year}"
        params = {
            "Season": season,
            "SeasonType": season_type_label,
            "Type": data_type,
        }

        try:
            # Fetch data from the API
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raise exception for HTTP errors
            response_json = response.json()
            stats = response_json.get("multi_row_table_data", [])

            # Skip if no data
            if not stats:
                print(f"No data found for {season} {season_type_label} {data_type}.")
                continue

            # Create DataFrame and add year column
            df = pd.DataFrame(stats)
            year_label = f"{year}ps" if season_type == 'ps' else str(year)
            df["year"] = year_label
            all_data.append(df)
            time.sleep(3)

            # Save to CSV if enabled
            if save_to_csv:
                # Add 'team' prefix for team data
                prefix = "team_" if data_type == "Team" else ""
                filename = f"data/{prefix}{year_label}_pbp.csv"
                df.to_csv(filename, index=False)
                print(f"Saved: {filename}")

        except Exception as e:
            print(f"Error fetching data for {season} {season_type_label} {data_type}: {e}")

    return all_data


os.makedirs("data", exist_ok=True)

for year in range(2009, 2026):  # inclusive of 2025
    for season_string in ["rs", "ps"]:
        # Fetch Player data
        print(f"Fetching {year} {season_string} Player data...")
        player_data = fetch_wnba_data(
            year,
            year,
            season_type=season_string,
            data_type='Player',
            save_to_csv=True
        )
        
        # Fetch Team data
        print(f"Fetching {year} {season_string} Team data...")
        team_data = fetch_wnba_data(
            year,
            year,
            season_type=season_string,
            data_type='Team',
            save_to_csv=True
        )