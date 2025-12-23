import pandas as pd
import unicodedata
import re
import os
from difflib import SequenceMatcher
import pandas as pd
import unicodedata
import re
import os
from difflib import SequenceMatcher

def normalize_name(name):
    if not isinstance(name, str): return ""
    name = name.lower()
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
    name = re.sub(r'[^a-z0-9 ]', '', name)
    return name.strip()

# Global map to persist matches across years
global_id_map = {} 

def match_players(df_ref, df_pbp, year_label):
    team_mapping = {'SAS': 'SAN'}
    df_ref = df_ref.copy()
    df_ref['team_mapped'] = df_ref['team'].replace(team_mapping)
    df_ref['norm_name'] = df_ref['player'].apply(normalize_name)
    
    df_pbp = df_pbp.copy()
    df_pbp['norm_name'] = df_pbp['Name'].apply(normalize_name)
    
    matches = []
    used_entity_ids = set()
    
    def add_match(ref_row, pbp_row, method):
        eid, pid,tid = pbp_row['EntityId'], ref_row['player_id'],pbp_row['TeamId']
        matches.append({
            'year_season': year_label,
            'player_id': pid,
            'EntityId': eid,
            'ref_name': ref_row['player'],
            'pbp_name': pbp_row['Name'],
            'team': ref_row['team'],
            'team_id':tid,
            'match_method': method
        })
        used_entity_ids.add(eid)
        global_id_map[pid] = eid

    # Pass 0: Persistent ID Match
    for _, row in df_ref.iterrows():
        if row['player_id'] in global_id_map:
            pbp_row = df_pbp[df_pbp['EntityId'] == global_id_map[row['player_id']]]
            if not pbp_row.empty:
                add_match(row, pbp_row.iloc[0], 'persistent_id')

    # Pass 1: Exact Name + Team
    remaining = df_ref[~df_ref['player_id'].isin([m['player_id'] for m in matches])]
    for _, row in remaining.iterrows():
        potential = df_pbp[(df_pbp['norm_name'] == row['norm_name']) & 
                           ((df_pbp['TeamAbbreviation'] == row['team_mapped']) | (row['team'] == 'TOT'))]
        if len(potential) == 1:
            add_match(row, potential.iloc[0], 'exact_team_name')

    # Pass 2: Global Name Match
    remaining = df_ref[~df_ref['player_id'].isin([m['player_id'] for m in matches])]
    for _, row in remaining.iterrows():
        potential = df_pbp[(df_pbp['norm_name'] == row['norm_name']) & (~df_pbp['EntityId'].isin(used_entity_ids))]
        if len(potential) == 1:
            add_match(row, potential.iloc[0], 'exact_name_global')

    # Pass 3: Fuzzy / Substring
    remaining = df_ref[~df_ref['player_id'].isin([m['player_id'] for m in matches])]
    for _, row in remaining.iterrows():
        potential = df_pbp[~df_pbp['EntityId'].isin(used_entity_ids)]
        if row['team'] != 'TOT':
            potential = potential[potential['TeamAbbreviation'] == row['team_mapped']]
        for _, p_row in potential.iterrows():
            if (row['norm_name'] in p_row['norm_name'] or p_row['norm_name'] in row['norm_name']) and len(row['norm_name']) > 5:
                add_match(row, p_row, 'substring')
                break
            elif SequenceMatcher(None, row['norm_name'], p_row['norm_name']).ratio() > 0.85:
                add_match(row, p_row, 'fuzzy')
                break

    # Pass 4: Lenient Stats (Tolerance: 5 min, 2 pts)
    remaining = df_ref[~df_ref['player_id'].isin([m['player_id'] for m in matches])]
    for _, row in remaining.iterrows():
        potential = df_pbp[~df_pbp['EntityId'].isin(used_entity_ids)]
        if row['team'] != 'TOT':
            potential = potential[potential['TeamAbbreviation'] == row['team_mapped']]
        
        stat_match = potential[
            (potential['GamesPlayed'] == row['g']) & 
            (abs(potential['Minutes'] - row['mp']) <= 5) & 
            (abs(potential['Points'] - row['pts']) <= 2)
        ]
        if len(stat_match) == 1:
            add_match(row, stat_match.iloc[0], 'stats_match_lenient')

    unmapped = df_ref[~df_ref['player_id'].isin([m['player_id'] for m in matches])]
    return pd.DataFrame(matches), unmapped

# Execution Loop for 2009-2025
all_matches = []
all_unmapped = []

for yr in range(2009, 2026):
    for suffix in ['', 'ps']: # Regular and Postseason
        f_ref, f_pbp = f"data/{yr}{suffix}_bballref.csv", f"data/{yr}{suffix}_pbp.csv"
        if os.path.exists(f_ref) and os.path.exists(f_pbp):
            m, u = match_players(pd.read_csv(f_ref), pd.read_csv(f_pbp), f"{yr}{suffix}")
            all_matches.append(m)
            all_unmapped.append(u.assign(year_season=f"{yr}{suffix}"))

# Save results
pd.concat(all_matches).to_csv('player_index_map.csv', index=False)


frame=pd.concat(all_matches)
frame=frame[['team_id','team','year_season']]

frame.to_csv('wteam_index.csv',index=False)

pd.concat(all_unmapped).to_csv('unmapped_players.csv', index=False)