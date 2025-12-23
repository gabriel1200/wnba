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

    master_set=[]
    
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





        combined= basic_stats(combined)
        combined=extra_fields(combined)

        combined.to_csv(out_name, index=False)
        combined['is_playoffs']=is_ps
        master_set.append(combined)
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
    return pd.concat(master_set)





def extra_fields(df, ps=False, avg_shooting_df=None, season_totals_df=None):
    """
    Add extra calculated fields to player statistics DataFrame.
    
    Args:
        df: Input DataFrame with player statistics
        ps: If True, process playoff data (uses different TS% baseline)
        avg_shooting_df: Pre-loaded avg_shooting.csv DataFrame
        season_totals_df: Pre-loaded season_totals.csv DataFrame
        playoff_rts_df: Pre-loaded playoffRTS.csv DataFrame
        late_df: Pre-loaded last_second.csv DataFrame
        pay_table_df: Pre-loaded pay_table.csv DataFrame
        
    Returns:
        DataFrame with additional calculated fields
    """
    print(len(df))
    df = df.copy()
    df = df.fillna(0)
    if '3pt And 1 Free Throw Trips' not in df.columns:
        df['3pt And 1 Free Throw Trips']=0

    

    
    # Calculate basic metrics
    df['TotalMisses'] = ((df['AtRimFGA'] - df['AtRimFGM']) + 
                        (df['ShortMidRangeFGA'] - df['ShortMidRangeFGM']) + 
                        (df['LongMidRangeFGA'] - df['LongMidRangeFGM']) + 
                        (df['Corner3FGA'] + df['Arc3FGA'] - df['Corner3FGM'] - df['Arc3FGM']))
    
    df['TotalOffRebounds'] = ((df['AtRimFGA'] - df['AtRimFGM']) * df['AtRimOffReboundedPct'] + 
                             (df['ShortMidRangeFGA'] - df['ShortMidRangeFGM']) * df['ShortMidRangeOffReboundedPct'] + 
                             (df['LongMidRangeFGA'] - df['LongMidRangeFGM']) * df['LongMidRangeOffReboundedPct'] + 
                             (df['Corner3FGA'] + df['Arc3FGA'] - df['Corner3FGM'] - df['Arc3FGM']) * df['ThreePtOffReboundedPct'])
    
    df['ProbabilityOffRebounded'] = df['TotalOffRebounds'] / df['TotalMisses']
    df['Shooting_FT_Possessions'] = ((df['TwoPtShootingFoulsDrawn'] - df['2pt And 1 Free Throw Trips']) +
                                     (df['ThreePtShootingFoulsDrawn'] - df['3pt And 1 Free Throw Trips']))
    
    df['FT_PERC'] = df['FtPoints'] / df['FTA']
    df['2TSA'] = (df['FG2A'] + (df['TwoPtShootingFoulsDrawn'] - df['2pt And 1 Free Throw Trips']) + 
                  df['NonShootingFoulsDrawn'])
    df['2FTA'] = ((df['TwoPtShootingFoulsDrawn'] - df['2pt And 1 Free Throw Trips']) * 2 + 
                  df['2pt And 1 Free Throw Trips'] + df['NonShootingFoulsDrawn'] * 2)
    df['3TSA'] = df['FG3A'] + (df['ThreePtShootingFoulsDrawn'] - df['3pt And 1 Free Throw Trips'])
    df['3FTA'] = (df['ThreePtShootingFoulsDrawn'] - df['3pt And 1 Free Throw Trips']) * 3 + df['3pt And 1 Free Throw Trips']
    df['2FTPoints'] = df['2FTA'] * df['FT_PERC']
    df['3FTPoints'] = df['3FTA'] * df['FT_PERC']
    df['2_Points'] = df['2FTPoints'] + df['FG2M'] * 2
    df['3_Points'] = df['3FTPoints'] + df['FG3M'] * 3
    df['2TS_percent'] = df['2_Points'] / (2 * df['2TSA'])
    df['3TS_percent'] = df['3_Points'] / (2 * df['3TSA'])
    df['PPG'] = df['Points'] / df['GamesPlayed']
    df['APG'] = df['Assists'] / df['GamesPlayed']
    
    # Teammate miss ORB%
    df['totalORB'] = df['OffThreePtRebounds'] + df['OffTwoPtRebounds']
    df['total_team_misses'] = df['totalORB'] / df['OffFGReboundPct']
    df['FGA'] = df['FG2A'] + df['FG3A']
    df['FGM'] = df['FG2M'] + df['FG3M']
    df['SelfOReb/100'] = (df['SelfOReb'] / 100) * df['OffPoss']
    
    df['total_teammate_misses'] = df['total_team_misses'] - (df['FGA'] - df['FGM'])
    df['teammatemissorb'] = df['totalORB'] - df['SelfOReb']
    df['TeammateMissORebPerc'] = df['teammatemissorb'] / df['total_teammate_misses']
    df['TeammateMissORebPerc'] = df['TeammateMissORebPerc'].replace([np.inf, -np.inf], np.nan).fillna(0)
    df['NonShooting_FT_Possessions'] = df['NonShootingFoulsDrawn']
    
    df['FT_Possessions'] = df['Shooting_FT_Possessions'] + df['NonShooting_FT_Possessions']
    df['FGA'] = df['FG2A'] + df['FG3A']
    
    df['2P_PERC'] = df['FG2M'] / df['FG2A']
    df['3P_PERC'] = df['FG3M'] / df['FG3A']
    df['3P_PERC'] = df['3P_PERC'].replace([np.inf, -np.inf], np.nan).fillna(0)
    df['FT_PERC'] = df['FtPoints'] / df['FTA']
    df['FTA_100'] = df['FTA'] / df['OffPoss'] * 100
    df['2PA_100'] = df['FG2A'] / df['OffPoss'] * 100
    df['3PA_100'] = df['FG3A'] / df['OffPoss'] * 100
    df['TSA'] = df['FGA'] + df['FT_Possessions']
    df['MPG'] = df['Minutes'] / df['GamesPlayed']
    df['TS%'] = df['Points'] / (2 * df['TSA'])
    df['TS_percent'] = (df['Points'] - (df['Technical Free Throw Trips'] * 0.8)) / (2 * df['TSA'])
    df['diff_in_points'] = df['FTA'] * 0.85 - df['FTA'] * df['FT_PERC']
    df['TS_eightyfive'] = ((df['Points'] + df['diff_in_points'] - (df['Technical Free Throw Trips'] * 0.8)) / 
                           (2 * df['TSA']))
    df['nontech_TS_percent'] = ((df['Points'] - (df['Technical Free Throw Trips'] * df['FT_PERC'])) / 
                                (2 * (df['TSA'])))
    
    df['mod_points'] = df['Points'] - (df['Technical Free Throw Trips'] * 0.8)
    df['mod_ts'] = df['mod_points'] / (2 * (df['TSA'] - df['SelfOReb']))
    
    # Optimized weighted average calculations using vectorized operations

    def calculate_weighted_avg_by_year(df, value_col, weight_col):
        """Calculate weighted average by year using vectorized operations."""
        # Create mask for valid values
        mask = (~np.isnan(df[value_col])) & (df[weight_col] > 0)
        
        # Group by year
        grouped = df[mask].groupby('year')
        
        # FIX: Add include_groups=False to the apply call
        weighted_sum = grouped.apply(
            lambda x: (x[value_col] * x[weight_col]).sum(), 
            include_groups=False
        )
        
        weight_sum = grouped[weight_col].sum()
        
        # Calculate weighted average
        weighted_avg = weighted_sum / weight_sum
        
        # Map back to original DataFrame
        return df['year'].map(weighted_avg)
    
    # Calculate weighted averages
    df['mod_ts_avg'] = calculate_weighted_avg_by_year(df, 'mod_ts', 'TSA')
    df['2ts_avg'] = calculate_weighted_avg_by_year(df, '2TS_percent', '2TSA')
    df['3ts_avg'] = calculate_weighted_avg_by_year(df, '3TS_percent', '3TSA')
    
    df['2rTS'] = df['2TS_percent'] - df['2ts_avg']
    df['3rTS'] = df['3TS_percent'] - df['3ts_avg']
    df['mod_rTS'] = (df['mod_ts'] - df['mod_ts_avg']) * 100
    
    # Load and merge avg shooting data
    if avg_shooting_df is None:
        avg = pd.read_csv('data/avg_shooting.csv')
    else:
        avg = avg_shooting_df.copy()
    
    avg['year'] = avg['Season'].str.split('-', expand=True)[0]
    avg['year'] = avg['year'].astype(int) + 1
    avg = avg[['year', 'TS%']]
    


    avg.columns = ['year', 'avg_ts']
    print(avg)
    df = df.merge(avg, on='year', how='left')
    

    
    # Rename columns (replace % with _pct)

   
    df['TSA100'] = 100 * (df['TSA'] / df['OffPoss'])
    
    # Drop TS_pct if exists and recreate
    if 'TS_pct' in df.columns:
        df.drop(columns=['TS_pct'], inplace=True)
    df['TS_pct'] = (df['Points'] - (df['Technical Free Throw Trips'] * 0.8)) / (2 * df['TSA'])
    
    if not ps:
        df['rTSPct'] = ((df['TS_pct'] - df['avg_ts']) * 100)
    else:
        df['rTSPct'] = ((df['TS_pct'] - (df['OPP_TS_PCT'] / 100)) * 100)
    
    df['Pts75'] = ((df['Points'] / df['OffPoss']) * 75).round(3)
    df['final_bp_tov_100'] = ((df['BadPassTurnovers'] + df['BadPassOutOfBoundsTurnovers']) / 
                             df['OffPoss']) * 100
    df['scoring_tov_100'] = ((df['Turnovers'] - df['BadPassTurnovers'] - 
                              df['BadPassOutOfBoundsTurnovers']) / df['OffPoss']) * 100
    df['tov_100'] = df['Turnovers'] / df['OffPoss'] * 100
    
    df['nonPassTOVPct'] = ((df['Turnovers'] - df['BadPassTurnovers'] - 
                           df['BadPassOutOfBoundsTurnovers']) / df['TSA']) * 100
    df['ProbabilityOffRebounded'] = df['ProbabilityOffRebounded'] * 100
    df['FG3A100'] = (df['FG3A'] / df['OffPoss']) * 100
    df['firstchanceperc'] = df['FirstChancePoints'] / (df['FirstChancePoints'] + df['SecondChancePoints'])


    return df


def basic_stats(df):
    """
    Calculate basic per-game statistics.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with basic stats added
    """
    df['basic_PPG'] = df['Points'] / df['GamesPlayed']
    df['basic_aPPG'] = df['AssistPoints'] / df['GamesPlayed']
    df['basic_APG'] = df['Assists'] / df['GamesPlayed']
    df['basic_ORB'] = df['OffRebounds'] / df['GamesPlayed']
    df['basic_DRB'] = df['DefRebounds'] / df['GamesPlayed']
    df['basic_REB'] = (df['DefRebounds'] + df['OffRebounds']) / df['GamesPlayed']
    df['basic_TOVPG'] = df['Turnovers'] / df['GamesPlayed']
    
    return df

if __name__ == "__main__":
    masterframe=process_wnba_pipeline()
    masterframe.to_csv('data/wnba_master.csv',index=False)