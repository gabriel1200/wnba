# WNBA Statistics Data Pipeline

A comprehensive Python pipeline for scraping, processing, and merging WNBA player and team statistics from multiple sources (Basketball Reference and PBP Stats) for seasons 2009-2025.

## Overview

This pipeline combines traditional box score statistics from Basketball Reference with advanced play-by-play metrics from PBP Stats to create a unified dataset with detailed player performance metrics including shooting efficiency, rebounding, and advanced analytics.

## Features

- **Multi-source data collection**: Scrapes both Basketball Reference and PBP Stats APIs
- **Automatic player matching**: Intelligent fuzzy matching algorithm to link players across data sources
- **Advanced metrics calculation**: Computes offensive/defensive ratings, true shooting percentages, and relative performance metrics
- **Regular season and playoffs support**: Handles both season types seamlessly
- **Years covered**: 2009-2025 (configurable)

## Pipeline Components

### 1. Data Collection Scripts

#### `wnba_totals.py`
Fetches player and team statistics from the PBP Stats API.

**Features:**
- Retrieves detailed play-by-play statistics
- Supports both regular season and playoff data
- Automatically saves data to CSV files with proper naming convention

**Usage:**
```python
python wnba_totals.py
```

**Output files:**
- `data/{year}_pbp.csv` - Regular season player stats
- `data/{year}ps_pbp.csv` - Playoff player stats
- `data/team_{year}_pbp.csv` - Regular season team stats
- `data/team_{year}ps_pbp.csv` - Playoff team stats

#### `bballref.py`
Scrapes traditional statistics from Basketball Reference.

**Features:**
- Extracts player totals and identifiers
- Preserves player URLs and IDs for cross-referencing
- Handles both regular season and playoff pages

**Usage:**
```python
python bballref.py
```

**Output files:**
- `data/{year}_bballref.csv` - Regular season totals
- `data/{year}ps_bballref.csv` - Playoff totals

### 2. Player Matching

#### `make_index.py`
Creates a master index mapping players between Basketball Reference and PBP Stats.

**Matching Strategy:**
1. **Persistent ID Match**: Uses previously established mappings
2. **Exact Name + Team**: Matches normalized names with team verification
3. **Global Name Match**: Matches names across the entire league
4. **Fuzzy/Substring Match**: Uses similarity scoring for close matches
5. **Stats-based Match**: Compares games played, minutes, and points

**Output files:**
- `player_index_map.csv` - Complete player ID mappings
- `wteam_index.csv` - Team ID mappings
- `unmapped_players.csv` - Players that couldn't be matched

### 3. Data Merging & Analytics

#### `merge_data.py`
Combines all data sources and calculates advanced metrics. Generates master file for all player seasons, rs and ps.


**Key Calculations:**
- **True Shooting %**: Accounts for all shot types and free throw efficiency
- **Relative TS%**: Performance vs. league average
- **Offensive/Defensive Ratings**: Points per 100 possessions
- **Net Rating**: Differential between offensive and defensive ratings
- **Rebound Percentages**: Self and teammate miss offensive rebounding
- **Shot Distribution**: Per-100 possession volume metrics
- **Turnover Breakdowns**: Pass turnovers vs. scoring turnovers

**Usage:**
```python
python merge_data.py
```

**Output files:**
- `data/{year}_combined.csv` - Season-specific merged data
- `data/{year}ps_combined.csv` - Playoff merged data
- `data/wnba_master.csv` - All seasons combined
- `data/avg_shooting.csv` - League shooting benchmarks

## Installation

### Requirements
```bash
pip install pandas numpy requests beautifulsoup4
```

### Setup
```bash
# Clone the repository
git clone <repository-url>
cd wnba-stats-pipeline

# Create data directory
mkdir data


# Run the pipeline
python wnba_totals.py      # Step 1: Fetch PBP Stats data
python bballref.py          # Step 2: Scrape Basketball Reference
python make_index.py        # Step 3: Create player mappings
python merge_data.py        # Step 4: Merge and calculate metrics
```

## Data Schema

### Key Fields in Final Output

**Identification:**
- `player_id` - Basketball Reference player ID
- `EntityId` - PBP Stats entity ID
- `year` - Season year
- `is_playoffs` - Boolean for playoff games

**Basic Stats:**
- `GamesPlayed`, `Minutes`, `Points`, `Assists`, `Rebounds`
- `FG2M/A`, `FG3M/A`, `FTA`, `FTPoints`

**Advanced Shooting:**
- `TS_percent` - True Shooting %
- `rTSPct` - Relative True Shooting % (vs. league average)
- `2TS_percent`, `3TS_percent` - Shot-type specific efficiency
- `mod_ts` - Modified TS% (adjusted for technical FTs and self-rebounds)

**Rating Metrics:**
- `ortg` - Offensive Rating (points per 100 possessions)
- `drtg` - Defensive Rating (opponent points per 100 possessions)
- `rortg`, `rdrtg` - Relative ratings vs. league average
- `NetRtg` - Net Rating (ortg - drtg)

**Volume Metrics:**
- `TSA` - True Shooting Attempts
- `FTA_100`, `2PA_100`, `3PA_100` - Shot attempts per 100 possessions
- `Pts75` - Points per 75 possessions

**Rebounding:**
- `totalORB` - Total offensive rebounds
- `TeammateMissORebPerc` - % of teammate misses rebounded
- `ProbabilityOffRebounded` - Self-shot offensive rebound probability

**Turnovers:**
- `tov_100` - Turnovers per 100 possessions
- `final_bp_tov_100` - Bad pass turnovers per 100
- `scoring_tov_100` - Non-pass turnovers per 100

