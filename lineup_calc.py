import pandas as pd
import numpy as np
import os
import re
from tqdm import tqdm

def calculate_basketball_percentages(df):
    """
    Calculates percentage-based metrics safely and filters 
    to keep only IDs and newly generated metrics.
    """
    result = df.copy()
    
    # Safe retrieval of core columns
    fga = result.get('FG2A', 0) + result.get('FG3A', 0)
    fgm = result.get('FG2M', 0) + result.get('FG3M', 0)
    result['FGA'] = fga
    result['FGM'] = fgm
    
    # Shooting Percentages
    if 'FG3A' in result.columns:
        result['Fg3Pct'] = (result['FG3M'] / result['FG3A']).fillna(0)
    if 'FG2A' in result.columns:
        result['Fg2Pct'] = (result['FG2M'] / result['FG2A']).fillna(0)
    
    # True Shooting with "w" factor
    points = result.get('Points', 0)
    fta = result.get('FTA', 0)
    and1_2pt = result.get('2pt And 1 Free Throw Trips', 0)
    and1_3pt = result.get('3pt And 1 Free Throw Trips', 0)
    
    w = np.where(fta > 0, (and1_2pt + 1.5 * and1_3pt + 0.44 * (fta - and1_2pt - and1_3pt)) / fta, 0.44)
    ts_denominator = 2 * (fga + w * fta)
    result['TsPct'] = np.where(ts_denominator > 0, points / ts_denominator, 0)
    
    # Location frequencies and accuracies
    locs = ['AtRim', 'ShortMidRange', 'LongMidRange', 'Corner3', 'Arc3']
    for loc in locs:
        fga_col, fgm_col = f"{loc}FGA", f"{loc}FGM"
        if fga_col in result.columns and fgm_col in result.columns:
            result[f"{loc}Frequency"] = np.where(fga > 0, result[fga_col] / fga, 0)
            result[f"{loc}Accuracy"] = np.where(result[fga_col] > 0, result[fgm_col] / result[fga_col], 0)
        else:
            result[f"{loc}Frequency"] = 0
            result[f"{loc}Accuracy"] = 0
            
    # Advanced Ratings
    off_poss = result.get('OffPoss', 0)
    def_poss = result.get('DefPoss', 0)
    result['ORtg'] = np.where(off_poss > 0, (result.get('Points', 0) / off_poss * 100), 0)
    result['DRtg'] = np.where(def_poss > 0, (result.get('opp_Points', 0) / def_poss * 100), 0)
    result['NetRtg'] = result['ORtg'] - result['DRtg']
    
    # --- Column Filtering ---
    # We only want ID info and the newly created calculated fields
    id_info = ['player_id', 'team_id', 'year_season', 'status', 'is_playoffs']
    metrics = ['FGA', 'FGM', 'Fg3Pct', 'Fg2Pct', 'TsPct', 'ORtg', 'DRtg', 'NetRtg']
    loc_stats = []
    for loc in locs:
        loc_stats.extend([f"{loc}Frequency", f"{loc}Accuracy"])
    
    # Add weighted average columns to the allowed list (calculated later in pipeline)
    weighted_metrics = ['OffFGReboundPct', 'DefFGReboundPct', 'OffTwoPtReboundPct', 'DefTwoPtReboundPct']
    
    keep_list = id_info + metrics + loc_stats + weighted_metrics
    final_cols = [c for c in keep_list if c in result.columns]
    
    return result[final_cols]

def calculate_weighted_average(df, value_col, weight_col):
    """Safely calculates weighted average to avoid RuntimeWarnings."""
    if weight_col not in df.columns or value_col not in df.columns:
        return 0
    weight_sum = df[weight_col].sum()
    if weight_sum == 0:
        return 0 
    df_clean = df.dropna(subset=[value_col, weight_col])
    return (df_clean[value_col] * df_clean[weight_col]).sum() / weight_sum

def run_on_off_pipeline():
    # 1. Load the player index
    if not os.path.exists('player_index_map.csv'):
        print("Error: player_index_map.csv not found.")
        return
    index = pd.read_csv('player_index_map.csv')
    all_results = []
    
    # 2. Group by Team-Year
    groups = list(index.groupby(['year_season', 'team_id']))
    
    # Progress Bar initialization
    for (year_season, team_id), group_players in tqdm(groups, desc="WNBA On/Off Analysis", unit="team"):
        is_ps = str(year_season).endswith('ps')
        suffix = "_ps" if is_ps else ""
        year_folder = re.sub(r'ps$', '', str(year_season))
        
        team_path = f"lineup_data/{year_folder}/{int(team_id)}{suffix}.csv"
        opp_path = f"lineup_data/{year_folder}/{int(team_id)}_vs{suffix}.csv"
        
        if not os.path.exists(team_path):
            continue
            
        df_team = pd.read_csv(team_path)
        
        # Merge opponent data to calculate Defensive Ratings
        if os.path.exists(opp_path):
            df_opp = pd.read_csv(opp_path).rename(columns=lambda x: f"opp_{x}" if x != 'EntityId' else x)
            df_merged = df_team.merge(df_opp, on='EntityId', how='left')
        else:
            df_merged = df_team.copy()
            for col in df_team.columns:
                if col != 'EntityId': df_merged[f"opp_{col}"] = 0

        # Pre-calculate weights for rebound rates
        df_merged['two_point_misses'] = df_merged.get('FG2A', 0) - df_merged.get('FG2M', 0)
        df_merged['opp_two_point_misses'] = df_merged.get('opp_FG2A', 0) - df_merged.get('opp_FG2M', 0)
        df_merged['fg_misses'] = (df_merged.get('FG2A', 0) + df_merged.get('FG3A', 0)) - \
                                 (df_merged.get('FG2M', 0) + df_merged.get('FG3M', 0))
        df_merged['opp_fg_misses'] = (df_merged.get('opp_FG2A', 0) + df_merged.get('opp_FG3A', 0)) - \
                                     (df_merged.get('opp_FG2M', 0) + df_merged.get('opp_FG3M', 0))

        weight_mapping = {
            'OffFGReboundPct': 'fg_misses',
            'DefFGReboundPct': 'opp_fg_misses',
            'OffTwoPtReboundPct': 'two_point_misses',
            'DefTwoPtReboundPct': 'opp_two_point_misses'
        }

        for _, player in group_players.iterrows():
            eid = str(player['EntityId'])
            on_mask = df_merged['EntityId'].astype(str).apply(lambda x: eid in x.split('-'))
            
            for status in ['ON', 'OFF']:
                mask = on_mask if status == 'ON' else ~on_mask
                df_subset = df_merged[mask]
                if df_subset.empty: continue
                
                # Aggregate totals for the subset
                totals = df_subset.sum(numeric_only=True)
                
                # Create a temporary DataFrame for calculation
                calc_df = pd.DataFrame([totals])
                calc_df['player_id'] = player['player_id']
                calc_df['team_id'] = team_id
                calc_df['year_season'] = year_season
                calc_df['status'] = status
                calc_df['is_playoffs'] = is_ps
                
                # Perform percentage calculations and column filtering
                filtered_row = calculate_basketball_percentages(calc_df).iloc[0].to_dict()
                
                # Re-insert weighted averages into the filtered dictionary
                for val_col, weight_col in weight_mapping.items():
                    if val_col in df_subset.columns:
                        filtered_row[val_col] = calculate_weighted_average(df_subset, val_col, weight_col)
                
                all_results.append(filtered_row)
                
    # 3. Save only the relevant ID and new Metric info
    if all_results:
        final_df = pd.DataFrame(all_results)
        final_df.to_csv('data/on_off_master.csv', index=False)
        print("\nComplete: on_off_master.csv generated with IDs and Metrics.")
    else:
        print("\nNo data was processed.")
if __name__ == "__main__":
    import re
    run_on_off_pipeline()
