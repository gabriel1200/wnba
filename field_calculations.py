"""
Field calculations module for computing advanced NBA player statistics.
"""
import pandas as pd
import numpy as np


def extra_fields(df, ps=False, avg_shooting_df=None, season_totals_df=None, 
                 playoff_rts_df=None, late_df=None, pay_table_df=None):
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
    df['nba_id'] = df['nba_id'].astype(int)
    
    # Drop columns if they exist
    cols_to_drop = []
    if 'TS_pct' in df.columns:
        cols_to_drop.append('TS_pct')
    if 'OPP_TS_PCT' in df.columns:
        cols_to_drop.extend(['OPP_TS_PCT', 'OPP_TS_PCT_x', 'OPP_TS_PCT_y'])
    if 'avg_ts' in df.columns:
        cols_to_drop.append('avg_ts')
    if 'late_fga' in df.columns:
        cols_to_drop.extend(['late_fga', 'late_efg'])
    if cols_to_drop:
        df.drop(columns=cols_to_drop, inplace=True)
    
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
        
        # Group by year and calculate weighted sum for numerator and denominator
        grouped = df[mask].groupby('year')
        weighted_sum = grouped.apply(lambda x: (x[value_col] * x[weight_col]).sum())
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
        avg = pd.read_csv('avg_shooting.csv')
    else:
        avg = avg_shooting_df.copy()
    
    avg['year'] = avg['Season'].str.split('-', expand=True)[0]
    avg['year'] = avg['year'].astype(int) + 1
    avg = avg[['year', 'TS%']]
    
    # Add season totals for years >= 2025
    if season_totals_df is None:
        season_totals = pd.read_csv('season_totals.csv')
    else:
        season_totals = season_totals_df.copy()
    
    season_totals = season_totals[['year', 'TsPct']].drop_duplicates()
    season_totals = season_totals.rename(columns={'TsPct': 'TS%'})
    season_totals = season_totals[season_totals['year'] >= 2025]
    
    avg = avg[avg['year'] != 2025]
    avg = pd.concat([avg, season_totals], ignore_index=True)
    avg.columns = ['year', 'avg_ts']
    print(avg)
    df = df.merge(avg, on='year', how='left')
    
    # Handle playoff-specific data
    second_path = 'last_second.csv'
    if ps:
        if playoff_rts_df is None:
            playoffs = pd.read_csv('playoffRTS.csv')
        else:
            playoffs = playoff_rts_df.copy()
        
        playoffs = playoffs[playoffs['round'] == 'All']
        playoffs = playoffs[playoffs['year'] != 'Car']
        playoffs['year'] = playoffs['year'].astype(int)
        playoffs = playoffs[['nba_id', 'year', 'OPP_TS_PCT']]
        playoffs.columns = ['nba_id', 'year', 'OPP_TS_PCT']
        playoffs['nba_id'] = playoffs['nba_id'].astype(int)
        df['nba_id'] = df['nba_id'].astype(int)
        df = df.merge(playoffs, on=['nba_id', 'year'], how='left')
        second_path = 'last_second_ps.csv'
    
    # Merge late game stats
    if late_df is None:
        late = pd.read_csv(second_path)
    else:
        late = late_df.copy()
        if ps and 'late_fga' not in late.columns:
            # Try to load from correct file
            late = pd.read_csv(second_path)
    
    late = late[['nba_id', 'year', 'late_fga', 'late_efg']]
    late['nba_id'] = late['nba_id'].astype(int)
    df = df.merge(late, on=['nba_id', 'year'], how='left')
    
    # Rename columns (replace % with _pct)
    columnmap = {col: col.replace('%', '_pct') for col in df.columns}
    df['late_fga_perc'] = df['late_fga'] / df['FGA']
    df.rename(columns=columnmap, inplace=True)
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
    df['grenadeperc'] = df['late_fga'] / df['FGA']
    
    # Merge pay table
    if pay_table_df is None:
        pay_table = pd.read_csv('pay_table.csv')
    else:
        pay_table = pay_table_df.copy()
    
    pay_table = pay_table[['Year', 'nba_id', 'Salary']]
    pay_table = pay_table.dropna(subset=['nba_id', 'Salary'])
    pay_table['year'] = pay_table['Year'].astype(int)
    pay_table['nba_id'] = pay_table['nba_id'].astype(int)
    pay_table['Salary'] = pay_table['Salary'].astype(int)
    
    df = df.merge(pay_table, on=['nba_id', 'year'], how='left')
    if 'Year' in df.columns:
        df.drop(columns=['Year'], inplace=True)
    df['Salary'] = df['Salary'].fillna(0)
    
    return df

