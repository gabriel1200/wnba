#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests
import plotly.graph_objects as go
import math
from scipy import stats
import string
import numpy as np
import time
from scipy.stats import zscore
import sys
import os
import glob

start_time = time.time()
SEASONYEAR = 2025
directory = f"lineup_data/{SEASONYEAR}"
ps = False

# Use glob to find all CSV files in the directory
csv_files = glob.glob(os.path.join(directory, "*.csv"))

# Loop through the list of files and delete based on `ps`

'''
for file in csv_files:
    filename = os.path.basename(file)
    if (ps and '_ps' in filename) or (not ps and '_ps' not in filename):
        try:
            os.remove(file)
            print(f"Deleted: {file}")
        except Exception as e:
            print(f"Error deleting {file}: {e}")
print("Relevant CSV files deleted.")
'''

time.sleep(1)


def lineuppull(team_id, season, opp=False, ps=False):
    term = "Opponent" if opp else "Team"
    s_type = "Playoffs" if ps else "Regular Season"

    wowy_url = "https://api.pbpstats.com/get-wowy-stats/wnba"  # Changed to WNBA
    print(team_id)
    wowy_params = {
        "TeamId": team_id,
        "Season": season,
        "SeasonType": s_type,
        "Type": term
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.183',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }

    wowy_response = requests.get(wowy_url, params=wowy_params, headers=headers)
    wowy = wowy_response.json()
    combos = wowy["multi_row_table_data"]
    frame_length = len(combos)
    df = pd.DataFrame(combos, index=[0]*frame_length)
    return df


def get_filename(team_id, year, opp=False, ps=False):
    """Generate filename based on parameters"""
    filename = f"{team_id}"
    if opp:
        filename += "_vs"
    if ps:
        filename += "_ps"
    filename += ".csv"
    return filename


def pull_onoff(years, opp=False, ps=False):
    count = 0
    # Read WNBA team index
    team_index = pd.read_csv('wteam_index.csv')
    team_index = team_index.drop_duplicates()
    team_index['team_id'] = team_index['team_id'].astype(int)
    all_frames = []

    for year in years:
        # Create year directory if it doesn't exist
        year_dir = f"data/{year}"
        os.makedirs(year_dir, exist_ok=True)

        # Filter by year_season with 'ps' suffix if playoffs
        year_season_str = f"{year}ps" if ps else f"{year}"
        season_index = team_index[team_index.year_season == year_season_str].reset_index(drop=True)
        season = f"{year}"  # WNBA uses single year format for API

        frames = []
        fail_list = []

        season_index.dropna(subset='team_id', inplace=True)

        for team_id in season_index.team_id.unique():
            # Generate filename for this team/year combination
            filename = get_filename(int(team_id), year, opp, ps)
            filepath = os.path.join(year_dir, filename)

            # Check if file already exists
            if os.path.exists(filepath):
                print(f"File already exists for team {team_id} in {year}, skipping...")
                # Optionally read existing file and add to frames
                existing_df = pd.read_csv(filepath)
                frames.append(existing_df)
                continue

            try:
                df = lineuppull(team_id, season, opp=opp, ps=ps)
                df = df.reset_index(drop=True)
                df['team_id'] = team_id
                df['year'] = year
                df['season'] = season
                df['team_vs'] = opp

                # Save individual team file
                df.to_csv(filepath, index=False)
                time.sleep(1)
                print(f"Saved data for team {team_id} in {year}")

                frames.append(df)
                count += 1

            except Exception as e:
                print(f"Error processing team {team_id} in {year}: {str(e)}")
                fail_list.append((team_id, year))

        if frames:
            year_frame = pd.concat(frames)
            all_frames.append(year_frame)
            print(f'Year {year} Completed')

    if fail_list:
        print("\nFailed to process the following team/year combinations:")
        for team, year in fail_list:
            print(f"Team: {team}, Year: {year}")

    return pd.concat(all_frames) if all_frames else pd.DataFrame()


# Run the data pulls
years = [i for i in range(SEASONYEAR, SEASONYEAR+1)]
print(years)

# Pull regular season data
df = pull_onoff(years, opp=False, ps=False) 
df = pull_onoff(years, opp=True, ps=False) 

# Pull playoff data
df_ps = pull_onoff(years, opp=False, ps=True) 
df_ps = pull_onoff(years, opp=True, ps=True) 

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Time taken: {elapsed_time} seconds")

# Optionally pull historical years
years = [i for i in range(2010, SEASONYEAR)]
df = pull_onoff(years, opp=False, ps=False) 
df = pull_onoff(years, opp=True, ps=False)


df = pull_onoff(years, opp=False, ps=True) 
df = pull_onoff(years, opp=True, ps=True)