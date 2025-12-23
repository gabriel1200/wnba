# WNBA Statistics Data Pipeline

A comprehensive Python pipeline for scraping, processing, and merging WNBA player and team statistics from multiple sources (Basketball Reference and PBP Stats) for seasons 2009-2025.

## Overview

This pipeline combines traditional box score statistics from Basketball Reference with advanced play-by-play metrics from PBP Stats to create a unified dataset with detailed player performance metrics including shooting efficiency, rebounding, and advanced analytics. The master file with all regular season and playoff rows can be found in data/wnba_master.csv

## Features

- **Multi-source data collection**: Scrapes both Basketball Reference and PBP Stats APIs
- **Automatic player matching**: Intelligent fuzzy matching algorithm to link players across data sources
- **Advanced metrics calculation**: Computes offensive/defensive ratings, true shooting percentages, and relative performance metrics
- **Regular season and playoffs support**: Handles both season types seamlessly
- **Years covered**: 2009-2025 (configurable)

## Quick Start

The entire core pipeline can be executed with a single command:
```bash
bash scrape.sh
```

This shell script runs the four main data collection and processing scripts in sequence:
1. `wnba_totals.py` - Fetches PBP Stats data
2. `bballref.py` - Scrapes Basketball Reference
3. `make_index.py` - Creates player mappings
4. `merge_data.py` - Merges data and calculates metrics

## Pipeline Components

### Core Pipeline (executed by `scrape.sh`)

#### 1. `wnba_totals.py`
Fetches player and team statistics from the PBP Stats API.

**Features:**
- Retrieves detailed play-by-play statistics
- Supports both regular season and playoff data
- Automatically saves data to CSV files with proper naming convention

**Output files:**
- `data/{year}_pbp.csv` - Regular season player stats
- `data/{year}ps_pbp.csv` - Playoff player stats
- `data/team_{year}_pbp.csv` - Regular season team stats
- `data/team_{year}ps_pbp.csv` - Playoff team stats

#### 2. `bballref.py`
Scrapes traditional statistics from Basketball Reference.

**Features:**
- Extracts player totals and identifiers
- Preserves player URLs and IDs for cross-referencing
- Handles both regular season and playoff pages

**Output files:**
- `data/{year}_bballref.csv` - Regular season totals
- `data/{year}ps_bballref.csv` - Playoff totals

#### 3. `make_index.py`
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

#### 4. `merge_data.py`
Combines all data sources and calculates advanced metrics. Generates master file for all player seasons, regular season and playoffs.

**Key Calculations:**
- **True Shooting %**: Accounts for all shot types and free throw efficiency
- **Relative TS%**: Performance vs. league average
- **Offensive/Defensive Ratings**: Points per 100 possessions
- **Net Rating**: Differential between offensive and defensive ratings
- **Rebound Percentages**: Self and teammate miss offensive rebounding
- **Shot Distribution**: Per-100 possession volume metrics
- **Turnover Breakdowns**: Pass turnovers vs. scoring turnovers

**Output files:**
- `data/{year}_combined.csv` - Season-specific merged data
- `data/{year}ps_combined.csv` - Playoff merged data
- `data/wnba_master.csv` - All seasons combined
- `data/avg_shooting.csv` - League shooting benchmarks

## Installation

### Requirements
```bash
pip install pandas numpy requests beautifulsoup4 scipy plotly
```

### Setup
```bash
# Clone the repository
git clone <repository-url>
cd wnba-stats-pipeline

# Create data directory
mkdir data

# Run the core pipeline
bash scrape.sh
```

## Data Schema

### Key Fields in Final Output

**Identification:**
- `player_id` - Basketball Reference player ID
- `EntityId` / `nba_id` - PBP Stats entity ID
- `pbp_name` - Player name from PBP Stats
- `year` - Season year
- `is_playoffs` - Boolean for playoff games
- `Pos` / `Position_Number` - Position (numeric: 1=G, 5=C)

**Basic Counting Stats:**
- `GamesPlayed`, `Minutes`, `Points`, `Assists`
- `OffRebounds`, `DefRebounds`, `Rebounds`
- `FG2M/A`, `FG3M/A`, `FGA`, `FGM`
- `FTA`, `FtPoints`
- `Turnovers`

**Basic Per-Game Stats:**
- `basic_PPG` - Points per game
- `basic_aPPG` - Assist points per game
- `basic_APG` - Assists per game
- `basic_ORB` - Offensive rebounds per game
- `basic_DRB` - Defensive rebounds per game
- `basic_REB` - Total rebounds per game
- `basic_TOVPG` - Turnovers per game
- `PPG`, `APG`, `MPG` - Standard per-game averages

**Advanced Shooting Efficiency:**
- `TS_percent` / `TS_pct` - True Shooting %
- `TS_eightyfive` - TS% if shooting 85% from FT line
- `nontech_TS_percent` - TS% excluding technical FTs
- `mod_ts` - Modified TS% (adjusted for technical FTs and self-rebounds)
- `rTSPct` - Relative True Shooting % (vs. league average)
- `2TS_percent` - Two-point True Shooting %
- `3TS_percent` - Three-point True Shooting %
- `2rTS` - Relative 2PT TS% vs. league average
- `3rTS` - Relative 3PT TS% vs. league average
- `mod_rTS` - Modified relative TS%
- `2P_PERC` - Two-point field goal %
- `3P_PERC` - Three-point field goal %
- `FT_PERC` - Free throw %

**Shot Type Components:**
- `2TSA` - Two-point True Shooting Attempts
- `3TSA` - Three-point True Shooting Attempts
- `2FTA` - Two-point free throw attempts (equivalent)
- `3FTA` - Three-point free throw attempts (equivalent)
- `2FTPoints` - Points from 2PT free throws
- `3FTPoints` - Points from 3PT free throws
- `2_Points` - Total points from 2PT attempts
- `3_Points` - Total points from 3PT attempts

**Rating Metrics:**
- `ortg` - Offensive Rating (points per 100 possessions)
- `drtg` - Defensive Rating (opponent points per 100 possessions)
- `rortg` - Relative offensive rating vs. league average
- `rdrtg` - Relative defensive rating vs. league average
- `NetRtg` - Net Rating (ortg - drtg)

**Volume Metrics (Per 100 Possessions):**
- `TSA` - True Shooting Attempts
- `TSA100` - TSA per 100 possessions
- `FTA_100` - Free throw attempts per 100
- `2PA_100` - Two-point attempts per 100
- `3PA_100` - Three-point attempts per 100
- `FG3A100` - Three-point attempts per 100 (alternate)
- `tov_100` - Turnovers per 100
- `final_bp_tov_100` - Bad pass turnovers per 100
- `scoring_tov_100` - Non-pass turnovers per 100
- `Pts75` - Points per 75 possessions

**Rebounding Metrics:**
- `totalORB` - Total offensive rebounds
- `TotalOffRebounds` - Offensive rebounds from all shot types
- `TotalMisses` - Total missed shots
- `ProbabilityOffRebounded` - % of own misses rebounded (self-rebound rate)
- `SelfOReb` - Self offensive rebounds
- `SelfOReb/100` - Self rebounds per 100 possessions
- `teammatemissorb` - Offensive rebounds of teammate misses
- `total_teammate_misses` - Total teammate missed shots
- `TeammateMissORebPerc` - % of teammate misses rebounded
- `OffFGReboundPct` - Team offensive rebound % when player on court

**Free Throw & Foul Metrics:**
- `Shooting_FT_Possessions` - Shooting foul free throw possessions
- `NonShooting_FT_Possessions` - Non-shooting foul possessions
- `FT_Possessions` - Total free throw possessions
- `TwoPtShootingFoulsDrawn` - 2PT shooting fouls drawn
- `ThreePtShootingFoulsDrawn` - 3PT shooting fouls drawn
- `NonShootingFoulsDrawn` - Non-shooting fouls drawn
- `2pt And 1 Free Throw Trips` - And-1 opportunities on 2PT shots
- `3pt And 1 Free Throw Trips` - And-1 opportunities on 3PT shots
- `Technical Free Throw Trips` - Technical foul free throws

**Turnover Breakdown:**
- `BadPassTurnovers` - Bad pass turnovers
- `BadPassOutOfBoundsTurnovers` - Bad pass out of bounds
- `nonPassTOVPct` - Non-pass turnover % of TSA

**Shot Location Data:**
- `AtRimFGA/FGM` - Attempts/makes at rim
- `ShortMidRangeFGA/FGM` - Short mid-range attempts/makes
- `LongMidRangeFGA/FGM` - Long mid-range attempts/makes
- `Corner3FGA/FGM` - Corner three attempts/makes
- `Arc3FGA/FGM` - Arc three attempts/makes
- Shot-specific offensive rebound percentages (e.g., `AtRimOffReboundedPct`)

**Possession & Point Distribution:**
- `OffPoss` - Offensive possessions
- `DefPoss` - Defensive possessions
- `FirstChancePoints` - Points on first chance possessions
- `SecondChancePoints` - Points on second chance possessions
- `firstchanceperc` - % of points from first chance
- `AssistPoints` - Points created via assists
- `OpponentPoints` - Opponent points when on court
- `PlusMinus` - Plus/minus differential

**Modified/Adjusted Metrics:**
- `mod_points` - Points adjusted for technical FTs
- `mod_ts_avg` - League average modified TS%
- `2ts_avg` - League average 2PT TS%
- `3ts_avg` - League average 3PT TS%
- `avg_ts` - League average TS% for the season
- `diff_in_points` - Point differential if shooting 85% FT


## Additional Data Collection Scripts

Beyond the core pipeline, this repository includes supplementary scripts for more granular data collection:

### `wnba_schedule.py`
Scrapes complete WNBA game schedules from the official WNBA API for seasons 2010-2024.

**Purpose:**
- Creates a master schedule file with game IDs, teams, dates, and season types
- Provides the foundation for play-by-play data collection
- Filters out preseason and all-star games

**Output:**
- `data/wnba_game_dates.csv` - Complete game schedule with metadata

**Usage:**
```bash
python wnba_schedule.py
```

### `wnba_playbyplay.py`
Retrieves detailed play-by-play data for individual games from the WNBA Stats API.

**Purpose:**
- Downloads granular event-level data for each possession
- Enables advanced game flow analysis and situational statistics
- Stores individual game files for efficient processing

**Requirements:**
- Requires `data/wnba_game_dates.csv` from `wnba_schedule.py`
- Automatically skips previously downloaded games

**Output:**
- `pbp_data/{gameId}.csv` - Individual play-by-play files per game

**Usage:**
```bash
python wnba_playbyplay.py
```

### `wnba_lineups.py`
Fetches lineup-level statistics and WOWY (With Or Without You) data from PBP Stats.

**Purpose:**
- Analyzes performance of specific player combinations
- Tracks on/off court impact for individual players
- Supports both team and opponent perspectives
- Handles historical data from 2010 onwards

**Features:**
- Pulls data for both regular season and playoffs
- Organizes files by year in `lineup_data/{year}/` directories
- Saves individual team files to avoid redundant API calls
- Supports analysis of lineup synergies and player value

**Output:**
- `lineup_data/{year}/{team_id}.csv` - Team lineup stats
- `lineup_data/{year}/{team_id}_vs.csv` - Opponent lineup stats
- `lineup_data/{year}/{team_id}_ps.csv` - Playoff lineup stats

**Usage:**
```bash
python wnba_lineups.py
```

**Configuration:**
- Modify `SEASONYEAR` variable to set target season
- Set `ps = True/False` for playoff vs. regular season focus
