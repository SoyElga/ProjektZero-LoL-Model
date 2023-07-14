# -*- coding: utf-8 -*-
"""
Oracle's Elixir Data Generator.

This script is intended to represent the main function to update data once per day.
Right now, it is storing data locally, but in the future it may write a NoSQL db
to AWS DynamoDB where it can be leveraged by other querying services.

This script is intended to be kicked off by a Cron job on a daily basis at 7 AM.

Please visit and support www.oracleselixir.com
Tim provides an invaluable service to the League community.
"""
# Housekeeping
import datetime as dt
from pathlib import Path
import pandas as pd
from typing import Tuple
import lol_modeling as lol
import oracles_elixir_legalane as oe


# Function Definitions
def enrich_dataset(player_data: pd.DataFrame,
                   team_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute all enrichment for team and player-based analytics and predictions.
    This includes DraftKings point totals, Team and Player-based elo, TrueSkill, and EGPM dominance.

    Parameters
    ----------
    player_data : pd.DataFrame
        DataFrame representing the output of oe.clean_data() split by player.
    team_data : pd.DataFrame
        DataFrame representing the output of oe.clean_data() split by team.

    Returns
    -------
    team_data: pd.DataFrame
        DataFrame containing team-based metrics and enrichment.
    player_data: pd.DataFrame
        DataFrame containing player-based metrics and enrichment.
    """

    # Enrich DraftKings Points Data
    team_data = lol.dk_enrich(team_data, entity='team')
    player_data = lol.dk_enrich(player_data, entity='player')

    #if df_checker.team_structure(team_data) and df_checker.player_structure(player_data):
    #    print("Added draftkings points data")
    print("Enrich DraftKings Points Data")

    # Enrich Elo Statistics
    player_data = lol.player_elo(player_data)

    team_data = lol.team_elo(team_data)
    team_data = lol.aggregate_player_elos(player_data, team_data)
    #if df_checker.team_structure(team_data) and df_checker.player_structure(player_data):
    #    print("Added Elo statistics")
    print("Enrich Elo Statistics")

    # Enrich Team TrueSkill
    player_data, team_data, ts_lookup = lol.trueskill_model(player_data, team_data, initial_sigma=2.75)
    print("Enrich Team TrueSkill")
    
    
    # EGPM Model - TrueSkill Normalized Earned Gold
    team_data = lol.egpm_model(team_data, "team")
    player_data = lol.egpm_model(player_data, "player")
    print("EGPM Model - TrueSkill Normalized Earned Gold")

    # EWM Model - Side Win Rates
    player_data = lol.ewm_model(player_data, "player")
    team_data = lol.ewm_model(team_data, "team")
    print("EWM Model - Side Win Rates")

    # Enrich Game Statistics
    team_data = lol.enrich_ema_statistics(team_data, "team")
    player_data = lol.enrich_ema_statistics(player_data, "player")
    print("Enrich Game Statistics")

    # Render CSV Files
    filepath = Path.cwd().parent
    team_data.drop('index', axis=1, inplace=True)
    team_data.to_csv(filepath.joinpath('data', 'interim', 'team_data.csv'), index=False)
    player_data.drop('index', axis=1, inplace=True)
    player_data.to_csv(filepath.joinpath('data', 'interim', 'player_data.csv'), index=False)

    # Flatten Data Frame / Render
    flattened_teams = team_data.sort_values(['teamid', 'date']).groupby('teamid').tail(1).reset_index(drop=True)
    flattened_teams = flattened_teams[["date", "league", "teamname",
                                       "team_elo_after", "trueskill_sum_mu",
                                       "trueskill_sigma_squared", "egpm_dominance_ema_after",
                                       "blue_side_ema_after", "red_side_ema_after",
                                       "kda_ema_after", "golddiffat15_ema_after",
                                       "csdiffat15_ema_after", "dkpoints_ema_after"]]
    flattened_teams = flattened_teams.rename(columns={'team_elo_after': 'team_elo',
                                                      'egpm_dominance_ema_after': 'egpm_dominance',
                                                      'kda_ema_after': 'kda',
                                                      'golddiffat15_ema_after': 'golddiffat15',
                                                      'csdiffat15_ema_after': 'csdiffat15',
                                                      'dkpoints_ema_after': 'dkpoints'})
    flattened_teams.to_csv(filepath.joinpath('data', 'processed', 'flattened_teams.csv'), index=False)

    flattened_players = (player_data.sort_values(['playerid', 'date']).groupby(['playerid', 'teamid'])
                         .tail(1).reset_index(drop=True))
    flattened_players = flattened_players[["date", "league", "teamname", "position",
                                           "playername", "playerid", "player_elo_after",
                                           "egpm_dominance_ema_after",
                                           "blue_side_ema_after", "red_side_ema_after",
                                           "kda_ema_after", "golddiffat15_ema_after",
                                           "csdiffat15_ema_after", "dkpoints_ema_after"]]
    flattened_players = flattened_players.rename(columns={'player_elo_after': 'player_elo',
                                                          'egpm_dominance_ema_after': 'egpm_dominance',
                                                          'kda_ema_after': 'kda',
                                                          'golddiffat15_ema_after': 'golddiffat15',
                                                          'csdiffat15_ema_after': 'csdiffat15',
                                                          'dkpoints_ema_after': 'dkpoints'})
    flattened_players[["trueskill_mu",
                       "trueskill_sigma"]] = flattened_players.apply(lambda row: [ts_lookup[row.playerid].mu,
                                                                                  ts_lookup[row.playerid].sigma],
                                                                     axis=1, result_type='expand')
    flattened_players.to_csv(filepath.joinpath('data', 'processed', 'flattened_players.csv'), index=False)

    return team_data, player_data


def main():
    # Define time frame for analytics
    current_year = dt.date.today().year
    years = [str(current_year), str(current_year - 1), str(current_year - 2)]

    # Download Data
    data = oe.download_data(years=years)

    # Remove Buggy Matches (both red/blue team listed as same team, invalid for elo/TrueSkill)
    # Games where a player is completly missing, this gets bugs in some of the models 
    player_missing_games = ['10074-10074_game_2', 'ESPORTSTMNT02_3170083', 
                            '10074-10074_game_1', 'ESPORTSTMNT02_3161035', 
                            'ESPORTSTMNT02_3171378', 'ESPORTSTMNT02_3169606', 
                            '10062-10062_game_3', 'ESPORTSTMNT02_3161006', 
                            '10062-10062_game_2', '10062-10062_game_1', 
                            'ESPORTSTMNT02_3160621']
    
    invalid_games = ['NA1/3754345055', 'NA1/3754344502',
                     'ESPORTSTMNT02/1890835', 'NA1/3669212337',
                     'NA1/3669211958', 'ESPORTSTMNT02/1890848',
                     'ESPORTSTMNT02_1932895', 'ESPORTSTMNT02_1932914',
                     'ESPORTSTMNT05_3220607']
    data = data[~data.gameid.isin(invalid_games + player_missing_games)].copy()

    # Clean/Format Data
    teams = oe.clean_data(data, split_on='team')
    players = oe.clean_data(data, split_on='player')

    teams, players = enrich_dataset(player_data=players, team_data=teams)

    return teams, players

if __name__ in ('__main__', '__builtin__', 'builtins'):
    start = dt.datetime.now()
    main()
    end = dt.datetime.now()
    elapsed = end - start
    print(f"Dataset generated in {elapsed}.")
