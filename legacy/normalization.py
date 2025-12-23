"""
Normalization and additional metrics calculation module.
"""
import pandas as pd
import numpy as np


def normalize_fields(df):
    """
    Normalize counting stats to per 100 possessions and per game rates.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with normalized fields
    """
    fields_to_normalize = [
        'Points', 'AssistPoints', 'Assists', 'OffRebounds', 'DefRebounds',
        'Rebounds', 'Turnovers', 'AtRimAssists', 'OFFD', 'Blocks', 'Steals',
        'DEFLECTIONS', 'AtRimFGA', 'ShortMidRangeFGA', 'LongMidRangeFGA',
        'FG3A', 'FTA', 'FGA', 'FTOVs', 'DEF_LOOSE_BALLS_RECOVERED'
    ]
    
    for field in fields_to_normalize:
        # Remove fields if they exist
        for suffix in ['_Per100', '_PerGame', '_Per75']:
            col_name = f'd_{field}{suffix}'
            if col_name in df.columns:
                df.drop(columns=[col_name], inplace=True)
        
        df[f'd_{field}_Per100'] = df[field] * 100 / df['OffPoss']
        df[f'd_{field}_PerGame'] = df[field] / df['GamesPlayed']
    
    df['Points_Created'] = df['Points'] + df['AssistPoints']
    df['d_Points_Created_Per100'] = df['Points_Created'] * 100 / df['OffPoss']
    df['d_Points_Created_PerGame'] = df['Points_Created'] / df['GamesPlayed']
    
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


def adj_hustle(df):
    """
    Calculate hustle statistics per 100 possessions.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with hustle stats added
    """
    df['Deflections/100'] = df['DEFLECTIONS'] * 100 / df['DefPoss']
    df['StealDeflectionRatio'] = df.apply(
        lambda x: '' if x['DEFLECTIONS'] == 0 or pd.isna(x['DEFLECTIONS']) 
        else x['Steals'] / x['DEFLECTIONS'], axis=1)
    df['SCREEN_ASSISTS_100'] = df['SCREEN_ASSISTS'] / df['OffPoss'] * 100
    df['SCREEN_AST_PTS_100'] = df['SCREEN_AST_PTS'] / df['OffPoss'] * 100
    df['OFF_LOOSE_BALLS_RECOVERED_100'] = df['OFF_LOOSE_BALLS_RECOVERED'] / df['OffPoss'] * 100
    df['DEF_LOOSE_BALLS_RECOVERED_100'] = df['DEF_LOOSE_BALLS_RECOVERED'] / df['DefPoss'] * 100
    
    return df


def calculate_offensive_metrics(df):
    """
    Calculate offensive load metrics including box creation and cTOV.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with offensive metrics added
    """
    # Calculate 3pt proficiency
    df['three_pt_prof'] = (2 / (1 + np.exp(-df['d_FG3A_Per100'])) - 1) * df['3P_PERC']
    
    # Calculate Box Creation using 3pt proficiency
    df['box_creation'] = (df['d_Assists_Per100'] * 0.1843 + 
                         (df['d_Points_Per100'] + df['d_Turnovers_Per100']) * 0.0969 - 
                         2.3021 * df['three_pt_prof'] +
                         0.0582 * (df['d_Assists_Per100'] * (df['d_Points_Per100'] + df['d_Turnovers_Per100']) * df['three_pt_prof']) - 
                         1.1942)
    
    # Calculate Offensive Load using Box Creation
    df['offensive_load'] = ((df['d_Assists_Per100'] - (0.38 * df['box_creation'])) * 0.75 + 
                           df['d_FGA_Per100'] + 
                           df['d_FTA_Per100'] * 0.44 + 
                           df['box_creation'] + 
                           df['d_Turnovers_Per100'])
    df['cTOV'] = df['d_Turnovers_Per100'] / df['offensive_load']
    
    # Calculate field goal percentages by zone
    df['RimFGPerc'] = df['AtRimFGM'] / df['AtRimFGA']
    df['ShortMidFGPerc'] = df['ShortMidRangeFGM'] / df['ShortMidRangeFGA']
    df['LongMidFGPerc'] = df['LongMidRangeFGM'] / df['LongMidRangeFGA']
    
    return df


def new_modify_df(df):
    """
    Add additional defensive and offensive metrics.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with new metrics added
    """
    df['scoring_tovs'] = df['Turnovers'] - df['BadPassTurnovers'] - df['BadPassOutOfBoundsTurnovers']
    df['ScoringTOV%'] = df['scoring_tovs'] / df['TSA100']
    df['true_badpass_tovs'] = df['BadPassTurnovers'] + df['BadPassOutOfBoundsTurnovers']
    df['true_badpass_tovs_100'] = df['true_badpass_tovs'] / df['OffPoss'] * 100
    df['scoring_tovs_100'] = df['scoring_tovs'] / df['OffPoss'] * 100
    df['TOV_100'] = df['Turnovers'] / df['OffPoss'] * 100
    df['OFFD'] = (df['Offensive Fouls Drawn'].fillna(0) + df['Charge Fouls Drawn'].fillna(0))
    df['OFFD_100'] = (df['OFFD'] / df['DefPoss'] * 100).fillna(0)
    df['Steals_100'] = (df['Steals'] / df['DefPoss'] * 100).fillna(0)
    
    df['FTOVs'] = (df['Steals'].fillna(0) + df['OFFD'].fillna(0))
    df['FTOVS_PG'] = df['FTOVs'] / df['GamesPlayed']
    
    df['FTOV_100'] = (df['OFFD_100'] + df['Steals_100']).fillna(0)
    df['STOPS'] = (df['FTOVs'] + df['RecoveredBlocks'].fillna(0))
    df['STOPS_100'] = (df['STOPS'] / df['DefPoss'] * 100).fillna(0)
    df['block_recov_percent'] = (df['RecoveredBlocks'] / df['Blocks']).fillna(0)
    df['Blocks_100'] = df['Blocks'] / df['DefPoss'] * 100
    df['Blocks_PG'] = df['Blocks'] / df['GamesPlayed']
    df['RecoveredBlocks_100'] = df['RecoveredBlocks'] / df['DefPoss'] * 100
    
    # Optimized weighted average calculations
    def calculate_weighted_avg_by_year(df, value_col, weight_col):
        """Calculate weighted average by year using vectorized operations."""
        mask = (~pd.isna(df[value_col])) & (df[weight_col] > 0)
        if not mask.any():
            return pd.Series(index=df.index, dtype='float64')
        
        grouped = df[mask].groupby('year')
        weighted_sum = grouped.apply(lambda x: (x[value_col] * x[weight_col]).sum())
        weight_sum = grouped[weight_col].sum()
        weighted_avg = weighted_sum / weight_sum
        
        return df['year'].map(weighted_avg)
    
    df['year_avg_ftov'] = calculate_weighted_avg_by_year(df, 'FTOV_100', 'DefPoss')
    df['year_avg_stops'] = calculate_weighted_avg_by_year(df, 'STOPS_100', 'DefPoss')
    df['rFTOV_100'] = df['FTOV_100'] - df['year_avg_ftov']
    df['rSTOPS_100'] = df['STOPS_100'] - df['year_avg_stops']
    
    df['SFC_100'] = df['ShootingFouls'] / df['DefPoss'] * 100
    df['year_avg_sfc'] = calculate_weighted_avg_by_year(df, 'SFC_100', 'DefPoss')
    df['rSFC_100'] = df['SFC_100'] - df['year_avg_sfc']
    
    df['DumbPenaltyFouls'] = df['NonShootingPenaltyNonTakeFouls']
    df['DumbPenaltyFouls_100'] = df['DumbPenaltyFouls'] / df['PenaltyDefPoss'] * 100
    df['year_avg_dpf'] = calculate_weighted_avg_by_year(df, 'DumbPenaltyFouls_100', 'PenaltyDefPoss')
    df['rDumbPenaltyFouls_100'] = df['DumbPenaltyFouls_100'] - df['year_avg_dpf']
    df['PenaltyDefPossPct'] = df['PenaltyDefPoss'] / df['DefPoss']
    
    # Calculate per 100 versions of assist types
    for col in ['AtRimAssists', 'ShortMidRangeAssists', 'LongMidRangeAssists', 'ThreePtAssists']:
        df[f'{col}/100'] = (df[col] / df['OffPoss']) * 100
    
    df['Assists/100'] = (df['Assists'] / df['OffPoss']) * 100
    df['AdjAssists/100'] = (df['AtRimAssists/100'] + df['ShortMidRangeAssists/100'] + 
                           df['LongMidRangeAssists/100'] + df['ThreePtAssists/100'] * 1.5)
    df['AssistPoints/100'] = (df['AtRimAssists/100'] * 2 + df['ShortMidRangeAssists/100'] * 2 + 
                              df['LongMidRangeAssists/100'] * 2 + df['ThreePtAssists/100'] * 3)
    
    return df


def add_new_fields(df):
    """
    Add points saved and TS added metrics.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with new fields added
    """
    df['rTS'] = (df['TS_pct'] - df['avg_ts'])
    
    # Convert columns to numeric
    numeric_columns = ['all_dfga', 'dif%', 'rim_dfga', 'rim_dif%', 'DefPoss', 'rTS', 'TSA100']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df[numeric_columns] = df[numeric_columns].fillna(0)
    df[numeric_columns] = df[numeric_columns].replace([np.inf, -np.inf], 0)
    
    # Vectorized calculations instead of apply
    df['points_saved'] = np.where(
        (df['all_dfga'].notna()) & (df['dif%'].notna()),
        df['all_dfga'] * df['dif%'] * -2 / 100,
        np.nan
    )
    df['rim_points_saved'] = np.where(
        (df['rim_dfga'].notna()) & (df['rim_dif%'].notna()),
        df['rim_dfga'] * df['rim_dif%'] * -2 / 100,
        np.nan
    )
    
    df['points_saved_100'] = np.where(
        (df['points_saved'].notna()) & (df['DefPoss'] > 0),
        (df['points_saved'] / df['DefPoss']) * 100,
        np.nan
    )
    df['rim_points_saved_100'] = np.where(
        (df['rim_points_saved'].notna()) & (df['DefPoss'] > 0),
        (df['rim_points_saved'] / df['DefPoss']) * 100,
        np.nan
    )
    
    df['TS_added_100'] = np.where(
        (df['rTS'].notna()) & (df['TSA100'].notna()),
        df['rTS'] * 2 * df['TSA100'],
        np.nan
    )
    
    return df


def create_dfg_breakdown_columns(df):
    """
    Create defensive field goal breakdown columns.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with DFG breakdown columns added
    """
    numeric_columns = [
        'all_dfga', 'DefPoss', 'rim_dfga', 'rim_acc_on', 'rim_acc_off',
        'rim_freq_on', 'rim_freq_off', 'ortg_on', 'drtg_on', 'netrtg_on',
        'ortg_off', 'drtg_off', 'netrtg_off'
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Optimized yearly average calculations
    def weighted_avg_transform(group, value_col, weight_col):
        """Calculate weighted average using vectorized operations."""
        mask = group[weight_col] > 0
        if not mask.any():
            return pd.Series(index=group.index, dtype='float64')
        weights = group.loc[mask, weight_col]
        values = group.loc[mask, value_col]
        weighted_avg = (values * weights).sum() / weights.sum()
        return pd.Series(weighted_avg, index=group.index)
    
    # Calculate yearly averages
    yearly_rim_acc_avg = df.groupby('year').apply(
        lambda x: (x['rim_acc_on'] * x['DefPoss']).sum() / x['DefPoss'].sum()
    )
    yearly_rim_freq_avg = df.groupby('year').apply(
        lambda x: (x['rim_freq_on'] * x['DefPoss']).sum() / x['DefPoss'].sum()
    )
    yearly_ortg_avg = df.groupby('year').apply(
        lambda x: (x['ortg_on'] * x['OffPoss']).sum() / x['OffPoss'].sum()
    )
    yearly_drtg_avg = df.groupby('year').apply(
        lambda x: (x['drtg_on'] * x['DefPoss']).sum() / x['DefPoss'].sum()
    )
    
    df['rim_acc_onoff'] = df['rim_acc_on'] - df['rim_acc_off']
    df['rim_freq_onoff'] = df['rim_freq_on'] - df['rim_freq_off']
    df['rortg_on_off'] = df['ortg_on'] - df['ortg_off']
    df['rdrtg_on_off'] = df['drtg_on'] - df['drtg_off']
    df['rdrtg_on_off'] = df['rdrtg_on_off'] * -1
    df['netrtg_on_off'] = df['netrtg_on'] - df['netrtg_off']
    
    # Calculate relative values using map instead of apply
    df['rim_acc_on'] = df['rim_acc_on'] - df['year'].map(yearly_rim_acc_avg)
    df['rim_freq_on'] = df['rim_freq_on'] - df['year'].map(yearly_rim_freq_avg)
    df['rortg_on'] = df['ortg_on'] - df['year'].map(yearly_ortg_avg)
    # Note: Original script has a bug - uses yearly_ortg_avg instead of yearly_drtg_avg for rdrtg_on
    # Matching original behavior to ensure identical outputs
    df['rdrtg_on'] = df['drtg_on'] - df['year'].map(yearly_ortg_avg)
    df['rdrtg_on'] = df['rdrtg_on'] * -1
    
    df['dfga/100'] = (df['all_dfga'] / df['DefPoss']) * 100
    df['rimdfga/100'] = (df['rim_dfga'] / df['DefPoss']) * 100
    
    rim_percentage_columns = ['rim_acc_on', 'rim_acc_onoff', 'rim_freq_onoff', 'rim_freq_on']
    for col in rim_percentage_columns:
        if col in df.columns:
            df[col] = df[col] * 100
    
    return df


def calculate_weighted_ts_averages(df):
    """
    Calculate weighted true shooting averages by playtype.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with weighted TS averages added
    """
    df = df.copy()
    df['final_creation_TSA100'] = pd.to_numeric(df['final_creation_TSA100'], errors='coerce')
    df['final_shooting_TSA100'] = pd.to_numeric(df['final_shooting_TSA100'], errors='coerce')
    df['final_ee_TSA100'] = pd.to_numeric(df['final_ee_TSA100'], errors='coerce')
    df['OffPoss'] = pd.to_numeric(df['OffPoss'], errors='coerce')
    
    df['final_creation_TSA'] = df['final_creation_TSA100'] * df['OffPoss'] / 100
    df['final_shooting_TSA'] = df['final_shooting_TSA100'] * df['OffPoss'] / 100
    df['final_ee_TSA'] = df['final_ee_TSA100'] * df['OffPoss'] / 100
    
    def weighted_avg(group, value_col, weight_col):
        values = pd.to_numeric(group[value_col], errors='coerce')
        weights = pd.to_numeric(group[weight_col], errors='coerce')
        mask = values.notna() & weights.notna() & (weights > 0)
        if mask.any():
            return np.average(values[mask], weights=weights[mask])
        return np.nan
    
    creation_avgs = df.groupby('year').apply(
        lambda x: weighted_avg(x, 'final_creation_TS', 'final_creation_TSA'),
        include_groups=False
    ).reset_index()
    creation_avgs.columns = ['year', 'final_creation_avg_ts']
    
    shooting_avgs = df.groupby('year').apply(
        lambda x: weighted_avg(x, 'final_shooting_TS', 'final_shooting_TSA'),
        include_groups=False
    ).reset_index()
    shooting_avgs.columns = ['year', 'final_shooting_avg_ts']
    
    ee_avgs = df.groupby('year').apply(
        lambda x: weighted_avg(x, 'final_ee_TS', 'final_ee_TSA'),
        include_groups=False
    ).reset_index()
    ee_avgs.columns = ['year', 'final_ee_avg_ts']
    
    df = df.merge(creation_avgs, on='year', how='left')
    df = df.merge(shooting_avgs, on='year', how='left')
    df = df.merge(ee_avgs, on='year', how='left')
    
    df['final_creation_rTS'] = df['final_creation_TS'] - df['final_creation_avg_ts']
    df['final_shooting_rTS'] = df['final_shooting_TS'] - df['final_shooting_avg_ts']
    df['final_ee_rTS'] = df['final_ee_TS'] - df['final_ee_avg_ts']
    
    df['SFC_pct'] = df['SFC_100'] / df['dfga/100'] * 100
    
    return df


def positional_added_values(df, weighted_avg_func):
    """
    Calculate position-adjusted added values.
    
    Args:
        df: Input DataFrame
        weighted_avg_func: Function to calculate weighted averages by group
        
    Returns:
        DataFrame with position-adjusted values added
    """
    ts_weighted_avg = weighted_avg_func(df, 'TS_added_100', 'Pos')
    rim_weighted_avg = weighted_avg_func(df, 'rim_points_saved_100', 'Pos')
    points_saved_weighted_avg = weighted_avg_func(df, 'points_saved_100', 'Pos')
    
    pos_map = {'PG': '1', 'SG': '2', 'SF': '3', 'PF': '4', 'C': '5'}
    ts_weighted_avg = ts_weighted_avg.rename(columns=pos_map)
    rim_weighted_avg = rim_weighted_avg.rename(columns=pos_map)
    points_saved_weighted_avg = points_saved_weighted_avg.rename(columns=pos_map)
    
    def interpolate_pos(row, pos_avg):
        pos_num = row['Position_Number']
        year = row['year']
        
        if pd.isna(pos_num) or pd.isna(year):
            return np.nan
        
        lower_pos = str(int(pos_num))
        upper_pos = str(min(int(pos_num) + 1, 5))
        fraction = pos_num - int(pos_num)
        
        lower_val = pos_avg.loc[year, lower_pos] if year in pos_avg.index and lower_pos in pos_avg.columns else 0
        upper_val = pos_avg.loc[year, upper_pos] if year in pos_avg.index and upper_pos in pos_avg.columns else 0
        
        return float(lower_val) * (1 - fraction) + float(upper_val) * fraction
    
    df['pos_TS_added_100'] = df.apply(lambda row: row['TS_added_100'] - interpolate_pos(row, ts_weighted_avg), axis=1)
    df['pos_rim_points_saved_100'] = df.apply(lambda row: row['rim_points_saved_100'] - interpolate_pos(row, rim_weighted_avg), axis=1)
    df['pos_points_saved_100'] = df.apply(lambda row: row['points_saved_100'] - interpolate_pos(row, points_saved_weighted_avg), axis=1)
    
    return df

