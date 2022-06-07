import csv
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
import Data_Processing as dp
import warnings
warnings.filterwarnings("ignore")


def numCols(type="base_player"):
    if (type == "base"):
        num_cols = ['stage', 'home_buildUpPlaySpeed', 'home_buildUpPlayPassing', 'home_chanceCreationPassing',
            'home_chanceCreationCrossing', 'home_chanceCreationShooting', 'home_defencePressure',
            'home_defenceAggression', 'home_defenceTeamWidth', 'away_buildUpPlaySpeed', 'away_buildUpPlayPassing',
            'away_chanceCreationPassing', 'away_chanceCreationCrossing', 'away_chanceCreationShooting',
            'away_defencePressure', 'away_defenceAggression', 'away_defenceTeamWidth']
    else:
        num_cols = ['stage', 'home_buildUpPlaySpeed', 'home_buildUpPlayPassing', 'home_chanceCreationPassing',
            'home_chanceCreationCrossing', 'home_chanceCreationShooting', 'home_defencePressure',
            'home_defenceAggression', 'home_defenceTeamWidth', 'away_buildUpPlaySpeed', 'away_buildUpPlayPassing',
            'away_chanceCreationPassing', 'away_chanceCreationCrossing', 'away_chanceCreationShooting',
            'away_defencePressure', 'away_defenceAggression', 'away_defenceTeamWidth', 'home_mean_height',
            'away_mean_height', 'home_mean_weight', 'away_mean_weight', 'home_mean_age', 'away_mean_age',
            'home_std_height', 'away_std_height', 'home_std_weight', 'away_std_weight', 'home_std_age', 'away_std_age']
    return num_cols


def catCols(type="base_player"):
    cat_cols = cat_cols = ['country_id', 'league_id', 'home_team_id', 'away_team_id', 'home_buildUpPlayDribblingClass',
            'home_buildUpPlayPositioningClass', 'home_chanceCreationPositioningClass', 'home_defenceDefenderLineClass',
            'away_buildUpPlayDribblingClass', 'away_buildUpPlayPositioningClass', 'away_chanceCreationPositioningClass',
            'away_defenceDefenderLineClass']
    return cat_cols


def encode_player_id(df):
    df_home_players = df[['home_player_1', 'home_player_2', 'home_player_3', 'home_player_4', 'home_player_5', 
                      'home_player_6', 'home_player_7', 'home_player_8', 'home_player_9', 'home_player_10',
                      'home_player_11']]
    df_away_players = df[['away_player_1', 'away_player_2', 'away_player_3', 'away_player_4', 'away_player_5',
                          'away_player_6', 'away_player_7', 'away_player_8', 'away_player_9', 'away_player_10', 
                          'away_player_11']]
    
    full_df_home_player = pd.concat((df['home_player_1'], df['home_player_2'], df['home_player_3'], df['home_player_4'],
           df['home_player_5'], df['home_player_6'], df['home_player_7'], df['home_player_8'],
           df['home_player_9'], df['home_player_10'], df['home_player_11']), axis=0)
    full_df_away_player = pd.concat((df['away_player_1'], df['away_player_2'], df['away_player_3'], df['away_player_4'],
               df['away_player_5'], df['away_player_6'], df['away_player_7'], df['away_player_8'],
               df['away_player_9'], df['away_player_10'], df['away_player_11']), axis=0)
    
    unique_home_players = pd.unique(full_df_home_player)
    unique_away_players = pd.unique(full_df_away_player)
    
    player_OHE = np.zeros((df.shape[0], len(unique_home_players)+len(unique_away_players)))
    for j in range(len(unique_home_players)):
        dfTrue = df_home_players == unique_home_players[j]
        obsHp = df.index[(dfTrue).any(axis=1)]
        player_OHE[obsHp,j] = 1

    for j in range(len(unique_away_players)):
        dfTrue = df_away_players == unique_away_players[j]
        obsAp = df.index[(dfTrue).any(axis=1)]
        player_OHE[obsAp,len(unique_home_players)+j] = 1
        
    return player_OHE


def handle_string(df):
    df['country_id'] = df['country_id'].astype(str)
    df['league_id'] = df['league_id'].astype(str)
    df['home_team_id'] = df['home_team_id'].astype(str)
    df['away_team_id'] = df['away_team_id'].astype(str)
    
    return df


def cat_encode(df, cat_cols):
    enc = OneHotEncoder(handle_unknown = 'ignore', sparse = False)
    enc.fit(df[cat_cols])
    cat_data = enc.transform(df[cat_cols]) 
    
    return cat_data


def handle_labels(df):
    df_Y = df['label'].copy()
    df_Y[df['label']=='H'] = 0
    df_Y[df['label']=='D'] = 1
    df_Y[df['label']=='A'] = 2
    Y = df_Y.to_numpy()
    Y = Y.astype(int)
    
    return Y


def feature_eng(df, type="base_player"):
    num_cols = numCols()
    cat_cols = catCols()
    if (type == "full"):
        player_OHE = encode_player_id(df)
    df = handle_string(df)
    cat_data = cat_encode(df, cat_cols)
    num_data = df[num_cols].to_numpy()
    
    if (type != "full"):
        X = np.concatenate((num_data, cat_data), axis=1)
    else:
        X = np.concatenate((num_data, cat_data, player_OHE), axis=1)
    
    Y = handle_labels(df)
    
    return X, Y