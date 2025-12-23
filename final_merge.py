import pandas as pd
import os

# 1. Load the data
master_path = 'data/wnba_master.csv'
on_off_path = 'data/on_off_master.csv'
new_path = 'data/updated_wnba_master.csv'

if os.path.exists(master_path) and os.path.exists(on_off_path):
    master_df = pd.read_csv(master_path)
    on_off_df = pd.read_csv(on_off_path)
    master_df=master_df[master_df.year>2009]
    on_off_df=on_off_df[~on_off_df.year_season.str.contains('2009')]
    # 2. Define the specific shooting metrics to extract
    # As per your request, we are only adding frequency and accuracy
    locs = ['AtRim', 'ShortMidRange', 'LongMidRange', 'Corner3', 'Arc3']
    shooting_stats = []
    for loc in locs:
        shooting_stats.extend([f"{loc}Frequency", f"{loc}Accuracy"])

    # 3. Pivot the On/Off data
    # We use player_id, team_id, and year_season as the unique keys
    pivot_on_off = on_off_df.pivot_table(
        index=['player_id', 'team_id', 'year_season'],
        columns='status',
        values=shooting_stats
    )

    # Flatten the multi-index columns (e.g., 'AtRimFrequency', 'ON' -> 'AtRimFrequency_ON')
    pivot_on_off.columns = [f"{col[0]}_{col[1]}" for col in pivot_on_off.columns]
    pivot_on_off.reset_index(inplace=True)

    # 4. Standardize 'team_id' to 'TeamId' for the merge
    pivot_on_off = pivot_on_off.rename(columns={'team_id': 'TeamId'})


    master_df['year']=master_df['year'].astype(int)

        
    # 1. Create a matching year_season key in the master dataframe
    # Mapping False -> '' (Regular) and True -> 'ps' (Playoffs)
    master_df['year_season_key'] = (
        master_df['year'].astype(str) + 
        master_df['is_playoffs'].map({True: 'ps', False: ''})
    )


    # 3. Merge using the specific year_season key to ensure a 1:1 match
    final_master = pd.merge(
        master_df,
        pivot_on_off,
        left_on=['player_id', 'TeamId', 'year_season_key'],
        right_on=['player_id', 'TeamId', 'year_season'],
        how='left'
    )

    # 4. Clean up the helper key
    final_master = final_master.drop(columns=['year_season_key'])

    # 6. Overwrite the master file with the new columns
    final_master.to_csv(new_path, index=False)
    print(f"Successfully added ON/OFF shooting stats to {new_path}")
else:
    print("Error: Could not find master or on/off CSV files in the data directory.")