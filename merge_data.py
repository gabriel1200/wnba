import pandas as pd
import numpy as np
import os
import re

def process_wnba_pipeline(data_folder='data'):
    # 1. Load the index map from the current directory
    if not os.path.exists('player_index_map.csv'):
        print("Error: player_index_map.csv not found in the current directory.")
        return
        
    index_df = pd.read_csv('player_index_map.csv')
    index_df['year_season'] = index_df['year_season'].astype(str)
    
    # 2. Identify files in the data subdirectory
    if not os.path.isdir(data_folder):
        print(f"Error: Subdirectory '{data_folder}' not found.")
        return
        
    files = os.listdir(data_folder)
    br_files = sorted([f for f in files if '_bballref.csv' in f])
    
    league_averages = []
    # Position mapping for WNBA (normalizing strings to 1-5 scale)
    pos_to_num = {'G': 1.0, 'G-F': 2.0, 'F-G': 2.0, 'F': 3.0, 'F-C': 4.0, 'C-F': 4.0, 'C': 5.0}
    
    for br_file in br_files:
        # Regex to handle years and playoffs (e.g., 2010_bballref.csv or 2009ps_bballref.csv)
        match = re.match(r'(\d+)(ps)?_bballref\.csv', br_file)
        if not match: continue
        
        year_str = match.group(1)
        is_ps = match.group(2) == 'ps'
        suffix = 'ps' if is_ps else ''
        pbp_file = f"{year_str}{suffix}_pbp.csv"
        team_pbp_file = f"team_{year_str}{suffix}_pbp.csv"
        
        br_path = os.path.join(data_folder, br_file)
        pbp_path = os.path.join(data_folder, pbp_file)
        team_pbp_path = os.path.join(data_folder, team_pbp_file)
        
        if not os.path.exists(pbp_path):
            print(f"Skipping {year_str}{suffix}: Corresponding PBP file not found.")
            continue
            
        print(f"Processing {year_str} {'Playoffs' if is_ps else 'Regular Season'}...")
        
        # Load datasets
        br_df = pd.read_csv(br_path)
        pbp_df = pd.read_csv(pbp_path)
        
        # Load team data if available for league averages
        team_df = None
        league_ortg = None
        league_drtg = None
        if os.path.exists(team_pbp_path):
            team_df = pd.read_csv(team_pbp_path)
            # Calculate league average offensive and defensive ratings
            total_points = team_df['Points'].sum()
            total_off_poss = team_df['OffPoss'].sum()
            total_opp_points = team_df['OpponentPoints'].sum()
            total_def_poss = team_df['DefPoss'].sum()
            
            if total_off_poss > 0:
                league_ortg = (total_points / total_off_poss) * 100
            if total_def_poss > 0:
                league_drtg = (total_opp_points / total_def_poss) * 100
                
            print(f"  League ortg: {league_ortg:.2f}, League drtg: {league_drtg:.2f}")
        
        # Determine mapping key from filename
        map_key = f"{year_str}{suffix}"
        year_index = index_df[index_df['year_season'] == map_key].copy()
        
        # Merge BBallRef with Index, then with PBP
        merged_br = br_df.merge(year_index[['player_id', 'EntityId', 'pbp_name']], 
                                on='player_id', how='left')
        combined = merged_br.merge(pbp_df, on='EntityId', how='inner')
        combined.drop_duplicates(subset='player_id',inplace=True)
        # Calculate offensive rating, defensive rating, and net rating
        combined['ortg'] = np.where(
            combined['OffPoss'] > 0,
            ((combined['OpponentPoints'] + combined['PlusMinus']) / combined['OffPoss']) * 100,
            np.nan
        )
        
        combined['drtg'] = np.where(
            combined['DefPoss'] > 0,
            (combined['OpponentPoints'] / combined['DefPoss']) * 100,
            np.nan
        )
        
        combined['NetRtg'] = combined['ortg'] - combined['drtg']
        
        # Calculate relative ratings if league averages are available
        if league_ortg is not None:
            combined['rortg'] = combined['ortg'] - league_ortg
        else:
            combined['rortg'] = np.nan
            
        if league_drtg is not None:
            combined['rdrtg'] = combined['drtg'] - league_drtg
        else:
            combined['rdrtg'] = np.nan
        
        # Standardize columns for field_calculations.py
        combined['year'] = int(year_str)
        combined['nba_id'] = combined['EntityId']
        if 'pos' in combined.columns:
            combined['Pos'] = combined['pos']
            combined['Position_Number'] = combined['pos'].map(pos_to_num).fillna(3.0) # Default to F
            
        # Save the combined file in the main directory
        out_name = f"data/{year_str}{suffix}_combined.csv"
        combined.to_csv(out_name, index=False)
        print(f"  -> Saved {out_name}")
        
        # Calculate League Shooting Baseline (Regular Season only)
        if not is_ps:
            calc_df = pbp_df.fillna(0)
            
            # Components for TSA (as defined in your field_calculations module)
            s_ft_poss = (calc_df['TwoPtShootingFoulsDrawn'] - calc_df['2pt And 1 Free Throw Trips']) + \
                        (calc_df['ThreePtShootingFoulsDrawn'] - calc_df['3pt And 1 Free Throw Trips'])
            ns_ft_poss = calc_df['NonShootingFoulsDrawn']
            
            # TSA = FGA (FG2A+FG3A) + FT Possession terms
            tsa = (calc_df['FG2A'] + calc_df['FG3A']) + s_ft_poss + ns_ft_poss
            
            # Points adjusted for estimated technical FT value
            pts_adj = calc_df['Points'] - (calc_df.get('Technical Free Throw Trips', 0) * 0.8)
            
            league_pts = pts_adj.sum()
            league_tsa = tsa.sum()
            
            if league_tsa > 0:
                league_ts_pct = league_pts / (2 * league_tsa)
                # Format Season as (year-1)-year (e.g., 2009-10) so the script increments it back to 2010
                season_str = f"{int(year_str)-1}-{year_str[-2:]}"
                league_averages.append({'Season': season_str, 'TS%': league_ts_pct})

    # Output the reference file for rTS calculation in the main directory
    if league_averages:
        pd.DataFrame(league_averages).to_csv('data/avg_shooting.csv', index=False)
        print("\nGenerated avg_shooting.csv for the WNBA pipeline.")

if __name__ == "__main__":
    process_wnba_pipeline()