import sqlite3
import pandas as pd
import numpy as np


def match_query():
    # Create a SQL connection to our SQLite database
    con = sqlite3.connect("database.sqlite")

    cur = con.cursor()

    df_match_data = pd.read_sql_query("""
    SELECT * FROM match""", con)
    
    return df_match_data


def base_query():
    # Create a SQL connection to our SQLite database
    con = sqlite3.connect("database.sqlite")

    cur = con.cursor()

    df_soccer_data = pd.read_sql_query("""
    WITH df_date_table AS ( \
        SELECT  m.id as match_id, \
                hta.team_api_id as home_team_id, \
                ata.team_api_id as away_team_id, \
                m.date as match_date, \
                max(hta.id) as ht_id, \
                max(ata.id) as at_id \
        FROM Match as m \
        INNER JOIN team_attributes as hta \
        ON (m.home_team_api_id = hta.team_api_id) \
        INNER JOIN team_attributes as ata \
        ON (m.away_team_api_id = ata.team_api_id) \
        WHERE (m.date > hta.date) AND (m.date > ata.date) \
        GROUP BY match_id, home_team_id, away_team_id, match_date) \
    SELECT  m.id as match_id, \
            m.country_id, \
            m.league_id, \
            m.season, \
            m.stage, \
            m.date as match_date, \
            m.home_team_api_id as home_team_id, \
            m.away_team_api_id as away_team_id, \
            m.home_team_goal, \
            m.away_team_goal, \
            m.goal, \
            m.shoton, \
            m.shotoff, \
            m.foulcommit, \
            m.card, \
            m.cross, \
            m.corner, \
            m.possession, \
            m.B365H, \
            m.B365D, \
            m.B365A, \
            m.BWH, \
            m.BWD, \
            m.BWA, \
            m.IWH, \
            m.IWD, \
            m.IWA, \
            m.LBH, \
            m.LBD, \
            m.LBA, \
            m.PSH, \
            m.PSD, \
            m.PSA, \
            m.WHH, \
            m.WHD, \
            m.WHA, \
            m.SJH, \
            m.SJD, \
            m.SJA, \
            m.VCH, \
            m.VCD, \
            m.VCA, \
            m.GBH, \
            m.GBD, \
            m.GBA, \
            m.BSH, \
            m.BSD, \
            m.BSA, \
            hta.date as home_start_date, \
            hta.buildUpPlaySpeed as home_buildUpPlaySpeed, \
            hta.buildUpPlaySpeedClass as home_buildUpPlaySpeedClass, \
            hta.buildUpPlayDribbling as home_buildUpPlayDribbling, \
            hta.buildUpPlayDribblingClass as home_buildUpPlayDribblingClass, \
            hta.buildUpPlayPassing as home_buildUpPlayPassing, \
            hta.buildUpPlayPassingClass as home_buildUpPlayPassingClass, \
            hta.buildUpPlayPositioningClass as home_buildUpPlayPositioningClass, \
            hta.chanceCreationPassing as home_chanceCreationPassing, \
            hta.chanceCreationPassingClass as home_chanceCreationPassingClass, \
            hta.chanceCreationCrossing as home_chanceCreationCrossing, \
            hta.chanceCreationCrossingClass as home_chanceCreationCrossingClass, \
            hta.chanceCreationShooting as home_chanceCreationShooting, \
            hta.chanceCreationShootingClass as home_chanceCreationShootingClass, \
            hta.chanceCreationPositioningClass as home_chanceCreationPositioningClass, \
            hta.defencePressure as home_defencePressure, \
            hta.defencePressureClass as home_defencePressureClass, \
            hta.defenceAggression as home_defenceAggression, \
            hta.defenceAggressionClass as home_defenceAggressionClass, \
            hta.defenceTeamWidth as home_defenceTeamWidth, \
            hta.defenceTeamWidthClass as home_defenceTeamWidthClass, \
            hta.defenceDefenderLineClass as home_defenceDefenderLineClass, \
            ata.date as away_start_date, \
            ata.buildUpPlaySpeed as away_buildUpPlaySpeed, \
            ata.buildUpPlaySpeedClass as away_buildUpPlaySpeedClass, \
            ata.buildUpPlayDribbling as away_buildUpPlayDribbling, \
            ata.buildUpPlayDribblingClass as away_buildUpPlayDribblingClass, \
            ata.buildUpPlayPassing as away_buildUpPlayPassing, \
            ata.buildUpPlayPassingClass as away_buildUpPlayPassingClass, \
            ata.buildUpPlayPositioningClass as away_buildUpPlayPositioningClass, \
            ata.chanceCreationPassing as away_chanceCreationPassing, \
            ata.chanceCreationPassingClass as away_chanceCreationPassingClass, \
            ata.chanceCreationCrossing as away_chanceCreationCrossing, \
            ata.chanceCreationCrossingClass as away_chanceCreationCrossingClass, \
            ata.chanceCreationShooting as away_chanceCreationShooting, \
            ata.chanceCreationShootingClass as away_chanceCreationShootingClass, \
            ata.chanceCreationPositioningClass as away_chanceCreationPositioningClass, \
            ata.defencePressure as away_defencePressure, \
            ata.defencePressureClass as away_defencePressureClass, \
            ata.defenceAggression as away_defenceAggression, \
            ata.defenceAggressionClass as away_defenceAggressionClass, \
            ata.defenceTeamWidth as away_defenceTeamWidth, \
            ata.defenceTeamWidthClass as away_defenceTeamWidthClass, \
            ata.defenceDefenderLineClass as away_defenceDefenderLineClass \
    FROM Match as m \
    INNER JOIN Country as c \
    ON (c.id = m.country_id) \
    INNER JOIN League as l \
    ON (l.id = m.league_id) \
    INNER JOIN team as ht \
    ON (m.home_team_api_id = ht.team_api_id) \
    INNER JOIN team as at \
    ON (m.away_team_api_id = at.team_api_id) \
    INNER JOIN team_attributes as hta \
    ON (m.home_team_api_id = hta.team_api_id) \
    INNER JOIN team_attributes as ata \
    ON (m.away_team_api_id = ata.team_api_id) \
    INNER JOIN df_date_table as ddt \
    ON (m.id = ddt.match_id AND hta.id = ddt.ht_id AND ata.id = ddt.at_id)""", con)

    # Be sure to close the connection
    con.close()
    
    return df_soccer_data


def base_player_query():
    # Create a SQL connection to our SQLite database
    con = sqlite3.connect("database.sqlite")

    cur = con.cursor()

    df_soccer_data = pd.read_sql_query("""
    WITH df_date_table AS ( \
        SELECT  m.id as match_id, \
                hta.team_api_id as home_team_id, \
                ata.team_api_id as away_team_id, \
                m.date as match_date, \
                max(hta.id) as ht_id, \
                max(ata.id) as at_id \
        FROM Match as m \
        INNER JOIN team_attributes as hta \
        ON (m.home_team_api_id = hta.team_api_id) \
        INNER JOIN team_attributes as ata \
        ON (m.away_team_api_id = ata.team_api_id) \
        WHERE (m.date > hta.date) AND (m.date > ata.date) \
        GROUP BY match_id, home_team_id, away_team_id, match_date) \
    SELECT  m.id as match_id, \
            m.country_id, \
            m.league_id, \
            m.season, \
            m.stage, \
            m.date as match_date, \
            m.home_team_api_id as home_team_id, \
            m.away_team_api_id as away_team_id, \
            m.home_team_goal, \
            m.away_team_goal, \
            m.goal, \
            m.shoton, \
            m.shotoff, \
            m.foulcommit, \
            m.card, \
            m.cross, \
            m.corner, \
            m.possession, \
            m.B365H, \
            m.B365D, \
            m.B365A, \
            m.BWH, \
            m.BWD, \
            m.BWA, \
            m.IWH, \
            m.IWD, \
            m.IWA, \
            m.LBH, \
            m.LBD, \
            m.LBA, \
            m.PSH, \
            m.PSD, \
            m.PSA, \
            m.WHH, \
            m.WHD, \
            m.WHA, \
            m.SJH, \
            m.SJD, \
            m.SJA, \
            m.VCH, \
            m.VCD, \
            m.VCA, \
            m.GBH, \
            m.GBD, \
            m.GBA, \
            m.BSH, \
            m.BSD, \
            m.BSA, \
            hta.date as home_start_date, \
            hta.buildUpPlaySpeed as home_buildUpPlaySpeed, \
            hta.buildUpPlaySpeedClass as home_buildUpPlaySpeedClass, \
            hta.buildUpPlayDribbling as home_buildUpPlayDribbling, \
            hta.buildUpPlayDribblingClass as home_buildUpPlayDribblingClass, \
            hta.buildUpPlayPassing as home_buildUpPlayPassing, \
            hta.buildUpPlayPassingClass as home_buildUpPlayPassingClass, \
            hta.buildUpPlayPositioningClass as home_buildUpPlayPositioningClass, \
            hta.chanceCreationPassing as home_chanceCreationPassing, \
            hta.chanceCreationPassingClass as home_chanceCreationPassingClass, \
            hta.chanceCreationCrossing as home_chanceCreationCrossing, \
            hta.chanceCreationCrossingClass as home_chanceCreationCrossingClass, \
            hta.chanceCreationShooting as home_chanceCreationShooting, \
            hta.chanceCreationShootingClass as home_chanceCreationShootingClass, \
            hta.chanceCreationPositioningClass as home_chanceCreationPositioningClass, \
            hta.defencePressure as home_defencePressure, \
            hta.defencePressureClass as home_defencePressureClass, \
            hta.defenceAggression as home_defenceAggression, \
            hta.defenceAggressionClass as home_defenceAggressionClass, \
            hta.defenceTeamWidth as home_defenceTeamWidth, \
            hta.defenceTeamWidthClass as home_defenceTeamWidthClass, \
            hta.defenceDefenderLineClass as home_defenceDefenderLineClass, \
            ata.date as away_start_date, \
            ata.buildUpPlaySpeed as away_buildUpPlaySpeed, \
            ata.buildUpPlaySpeedClass as away_buildUpPlaySpeedClass, \
            ata.buildUpPlayDribbling as away_buildUpPlayDribbling, \
            ata.buildUpPlayDribblingClass as away_buildUpPlayDribblingClass, \
            ata.buildUpPlayPassing as away_buildUpPlayPassing, \
            ata.buildUpPlayPassingClass as away_buildUpPlayPassingClass, \
            ata.buildUpPlayPositioningClass as away_buildUpPlayPositioningClass, \
            ata.chanceCreationPassing as away_chanceCreationPassing, \
            ata.chanceCreationPassingClass as away_chanceCreationPassingClass, \
            ata.chanceCreationCrossing as away_chanceCreationCrossing, \
            ata.chanceCreationCrossingClass as away_chanceCreationCrossingClass, \
            ata.chanceCreationShooting as away_chanceCreationShooting, \
            ata.chanceCreationShootingClass as away_chanceCreationShootingClass, \
            ata.chanceCreationPositioningClass as away_chanceCreationPositioningClass, \
            ata.defencePressure as away_defencePressure, \
            ata.defencePressureClass as away_defencePressureClass, \
            ata.defenceAggression as away_defenceAggression, \
            ata.defenceAggressionClass as away_defenceAggressionClass, \
            ata.defenceTeamWidth as away_defenceTeamWidth, \
            ata.defenceTeamWidthClass as away_defenceTeamWidthClass, \
            ata.defenceDefenderLineClass as away_defenceDefenderLineClass, \
            hp1.height as hp1_height, \
            hp2.height as hp2_height, \
            hp3.height as hp3_height, \
            hp4.height as hp4_height, \
            hp5.height as hp5_height, \
            hp6.height as hp6_height, \
            hp7.height as hp7_height, \
            hp8.height as hp8_height, \
            hp9.height as hp9_height, \
            hp10.height as hp10_height, \
            hp11.height as hp11_height, \
            ap1.height as ap1_height, \
            ap2.height as ap2_height, \
            ap3.height as ap3_height, \
            ap4.height as ap4_height, \
            ap5.height as ap5_height, \
            ap6.height as ap6_height, \
            ap7.height as ap7_height, \
            ap8.height as ap8_height, \
            ap9.height as ap9_height, \
            ap10.height as ap10_height, \
            ap11.height as ap11_height, \
            hp1.weight as hp1_weight, \
            hp2.weight as hp2_weight, \
            hp3.weight as hp3_weight, \
            hp4.weight as hp4_weight, \
            hp5.weight as hp5_weight, \
            hp6.weight as hp6_weight, \
            hp7.weight as hp7_weight, \
            hp8.weight as hp8_weight, \
            hp9.weight as hp9_weight, \
            hp10.weight as hp10_weight, \
            hp11.weight as hp11_weight, \
            ap1.weight as ap1_weight, \
            ap2.weight as ap2_weight, \
            ap3.weight as ap3_weight, \
            ap4.weight as ap4_weight, \
            ap5.weight as ap5_weight, \
            ap6.weight as ap6_weight, \
            ap7.weight as ap7_weight, \
            ap8.weight as ap8_weight, \
            ap9.weight as ap9_weight, \
            ap10.weight as ap10_weight, \
            ap11.weight as ap11_weight, \
            Julianday(m.date) - JulianDay(hp1.birthday) as hp1_age, \
            Julianday(m.date) - JulianDay(hp2.birthday) as hp2_age, \
            Julianday(m.date) - JulianDay(hp3.birthday) as hp3_age, \
            Julianday(m.date) - JulianDay(hp4.birthday) as hp4_age, \
            Julianday(m.date) - JulianDay(hp5.birthday) as hp5_age, \
            Julianday(m.date) - JulianDay(hp6.birthday) as hp6_age, \
            Julianday(m.date) - JulianDay(hp7.birthday) as hp7_age, \
            Julianday(m.date) - JulianDay(hp8.birthday) as hp8_age, \
            Julianday(m.date) - JulianDay(hp9.birthday) as hp9_age, \
            Julianday(m.date) - JulianDay(hp10.birthday) as hp10_age, \
            Julianday(m.date) - JulianDay(hp11.birthday) as hp11_age, \
            Julianday(m.date) - JulianDay(ap1.birthday) as ap1_age, \
            Julianday(m.date) - JulianDay(ap2.birthday) as ap2_age, \
            Julianday(m.date) - JulianDay(ap3.birthday) as ap3_age, \
            Julianday(m.date) - JulianDay(ap4.birthday) as ap4_age, \
            Julianday(m.date) - JulianDay(ap5.birthday) as ap5_age, \
            Julianday(m.date) - JulianDay(ap6.birthday) as ap6_age, \
            Julianday(m.date) - JulianDay(ap7.birthday) as ap7_age, \
            Julianday(m.date) - JulianDay(ap8.birthday) as ap8_age, \
            Julianday(m.date) - JulianDay(ap9.birthday) as ap9_age, \
            Julianday(m.date) - JulianDay(ap10.birthday) as ap10_age, \
            Julianday(m.date) - JulianDay(ap11.birthday) as ap11_age \
    FROM Match as m \
    INNER JOIN Country as c \
    ON (c.id = m.country_id) \
    INNER JOIN League as l \
    ON (l.id = m.league_id) \
    INNER JOIN team as ht \
    ON (m.home_team_api_id = ht.team_api_id) \
    INNER JOIN team as at \
    ON (m.away_team_api_id = at.team_api_id) \
    INNER JOIN team_attributes as hta \
    ON (m.home_team_api_id = hta.team_api_id) \
    INNER JOIN team_attributes as ata \
    ON (m.away_team_api_id = ata.team_api_id) \
    INNER JOIN df_date_table as ddt \
    ON (m.id = ddt.match_id AND hta.id = ddt.ht_id AND ata.id = ddt.at_id)
    LEFT JOIN player as hp1 \
    ON (CAST(m.home_player_1 as INT) = hp1.player_api_id) \
    LEFT JOIN player as hp2 \
    ON (CAST(m.home_player_2 as INT) = hp2.player_api_id) \
    LEFT JOIN player as hp3 \
    ON (CAST(m.home_player_3 as INT) = hp3.player_api_id) \
    LEFT JOIN player as hp4 \
    ON (CAST(m.home_player_4 as INT) = hp4.player_api_id) \
    LEFT JOIN player as hp5 \
    ON (CAST(m.home_player_5 as INT) = hp5.player_api_id) \
    LEFT JOIN player as hp6 \
    ON (CAST(m.home_player_6 as INT) = hp6.player_api_id)
    LEFT JOIN player as hp7 \
    ON (CAST(m.home_player_7 as INT) = hp7.player_api_id) \
    LEFT JOIN player as hp8 \
    ON (CAST(m.home_player_8 as INT) = hp8.player_api_id) \
    LEFT JOIN player as hp9 \
    ON (CAST(m.home_player_9 as INT) = hp9.player_api_id) \
    LEFT JOIN player as hp10 \
    ON (CAST(m.home_player_10 as INT) = hp10.player_api_id) \
    LEFT JOIN player as hp11 \
    ON (CAST(m.home_player_11 as INT) = hp11.player_api_id) \
    LEFT JOIN player as ap1 \
    ON (CAST(m.away_player_1 as INT) = ap1.player_api_id) \
    LEFT JOIN player as ap2 \
    ON (CAST(m.away_player_2 as INT) = ap2.player_api_id) \
    LEFT JOIN player as ap3 \
    ON (CAST(m.away_player_3 as INT) = ap3.player_api_id) \
    LEFT JOIN player as ap4 \
    ON (CAST(m.away_player_4 as INT) = ap4.player_api_id) \
    LEFT JOIN player as ap5 \
    ON (CAST(m.away_player_5 as INT) = ap5.player_api_id) \
    LEFT JOIN player as ap6 \
    ON (CAST(m.away_player_6 as INT) = ap6.player_api_id) \
    LEFT JOIN player as ap7 \
    ON (CAST(m.away_player_7 as INT) = ap7.player_api_id) \
    LEFT JOIN player as ap8 \
    ON (CAST(m.away_player_8 as INT) = ap8.player_api_id) \
    LEFT JOIN player as ap9 \
    ON (CAST(m.away_player_9 as INT) = ap9.player_api_id) \
    LEFT JOIN player as ap10 \
    ON (CAST(m.away_player_10 as INT) = ap10.player_api_id) \
    LEFT JOIN player as ap11 \
    ON (CAST(m.away_player_11 as INT) = ap11.player_api_id) """, con)

    # Be sure to close the connection
    con.close()
    
    return df_soccer_data


def full_query():
    # Create a SQL connection to our SQLite database
    con = sqlite3.connect("database.sqlite")

    cur = con.cursor()

    df_soccer_data = pd.read_sql_query("""
    WITH df_date_table AS ( \
        SELECT  m.id as match_id, \
                hta.team_api_id as home_team_id, \
                ata.team_api_id as away_team_id, \
                m.date as match_date, \
                max(hta.id) as ht_id, \
                max(ata.id) as at_id \
        FROM Match as m \
        INNER JOIN team_attributes as hta \
        ON (m.home_team_api_id = hta.team_api_id) \
        INNER JOIN team_attributes as ata \
        ON (m.away_team_api_id = ata.team_api_id) \
        WHERE (m.date > hta.date) AND (m.date > ata.date) \
        GROUP BY match_id, home_team_id, away_team_id, match_date) \
    SELECT  m.id as match_id, \
            m.country_id, \
            m.league_id, \
            m.season, \
            m.stage, \
            m.date as match_date, \
            m.home_team_api_id as home_team_id, \
            m.away_team_api_id as away_team_id, \
            m.home_team_goal, \
            m.away_team_goal, \
            m.goal, \
            m.shoton, \
            m.shotoff, \
            m.foulcommit, \
            m.card, \
            m.cross, \
            m.corner, \
            m.possession, \
            m.B365H, \
            m.B365D, \
            m.B365A, \
            m.BWH, \
            m.BWD, \
            m.BWA, \
            m.IWH, \
            m.IWD, \
            m.IWA, \
            m.LBH, \
            m.LBD, \
            m.LBA, \
            m.PSH, \
            m.PSD, \
            m.PSA, \
            m.WHH, \
            m.WHD, \
            m.WHA, \
            m.SJH, \
            m.SJD, \
            m.SJA, \
            m.VCH, \
            m.VCD, \
            m.VCA, \
            m.GBH, \
            m.GBD, \
            m.GBA, \
            m.BSH, \
            m.BSD, \
            m.BSA, \
            hta.date as home_start_date, \
            hta.buildUpPlaySpeed as home_buildUpPlaySpeed, \
            hta.buildUpPlaySpeedClass as home_buildUpPlaySpeedClass, \
            hta.buildUpPlayDribbling as home_buildUpPlayDribbling, \
            hta.buildUpPlayDribblingClass as home_buildUpPlayDribblingClass, \
            hta.buildUpPlayPassing as home_buildUpPlayPassing, \
            hta.buildUpPlayPassingClass as home_buildUpPlayPassingClass, \
            hta.buildUpPlayPositioningClass as home_buildUpPlayPositioningClass, \
            hta.chanceCreationPassing as home_chanceCreationPassing, \
            hta.chanceCreationPassingClass as home_chanceCreationPassingClass, \
            hta.chanceCreationCrossing as home_chanceCreationCrossing, \
            hta.chanceCreationCrossingClass as home_chanceCreationCrossingClass, \
            hta.chanceCreationShooting as home_chanceCreationShooting, \
            hta.chanceCreationShootingClass as home_chanceCreationShootingClass, \
            hta.chanceCreationPositioningClass as home_chanceCreationPositioningClass, \
            hta.defencePressure as home_defencePressure, \
            hta.defencePressureClass as home_defencePressureClass, \
            hta.defenceAggression as home_defenceAggression, \
            hta.defenceAggressionClass as home_defenceAggressionClass, \
            hta.defenceTeamWidth as home_defenceTeamWidth, \
            hta.defenceTeamWidthClass as home_defenceTeamWidthClass, \
            hta.defenceDefenderLineClass as home_defenceDefenderLineClass, \
            ata.date as away_start_date, \
            ata.buildUpPlaySpeed as away_buildUpPlaySpeed, \
            ata.buildUpPlaySpeedClass as away_buildUpPlaySpeedClass, \
            ata.buildUpPlayDribbling as away_buildUpPlayDribbling, \
            ata.buildUpPlayDribblingClass as away_buildUpPlayDribblingClass, \
            ata.buildUpPlayPassing as away_buildUpPlayPassing, \
            ata.buildUpPlayPassingClass as away_buildUpPlayPassingClass, \
            ata.buildUpPlayPositioningClass as away_buildUpPlayPositioningClass, \
            ata.chanceCreationPassing as away_chanceCreationPassing, \
            ata.chanceCreationPassingClass as away_chanceCreationPassingClass, \
            ata.chanceCreationCrossing as away_chanceCreationCrossing, \
            ata.chanceCreationCrossingClass as away_chanceCreationCrossingClass, \
            ata.chanceCreationShooting as away_chanceCreationShooting, \
            ata.chanceCreationShootingClass as away_chanceCreationShootingClass, \
            ata.chanceCreationPositioningClass as away_chanceCreationPositioningClass, \
            ata.defencePressure as away_defencePressure, \
            ata.defencePressureClass as away_defencePressureClass, \
            ata.defenceAggression as away_defenceAggression, \
            ata.defenceAggressionClass as away_defenceAggressionClass, \
            ata.defenceTeamWidth as away_defenceTeamWidth, \
            ata.defenceTeamWidthClass as away_defenceTeamWidthClass, \
            ata.defenceDefenderLineClass as away_defenceDefenderLineClass, \
            hp1.height as hp1_height, \
            hp2.height as hp2_height, \
            hp3.height as hp3_height, \
            hp4.height as hp4_height, \
            hp5.height as hp5_height, \
            hp6.height as hp6_height, \
            hp7.height as hp7_height, \
            hp8.height as hp8_height, \
            hp9.height as hp9_height, \
            hp10.height as hp10_height, \
            hp11.height as hp11_height, \
            ap1.height as ap1_height, \
            ap2.height as ap2_height, \
            ap3.height as ap3_height, \
            ap4.height as ap4_height, \
            ap5.height as ap5_height, \
            ap6.height as ap6_height, \
            ap7.height as ap7_height, \
            ap8.height as ap8_height, \
            ap9.height as ap9_height, \
            ap10.height as ap10_height, \
            ap11.height as ap11_height, \
            hp1.weight as hp1_weight, \
            hp2.weight as hp2_weight, \
            hp3.weight as hp3_weight, \
            hp4.weight as hp4_weight, \
            hp5.weight as hp5_weight, \
            hp6.weight as hp6_weight, \
            hp7.weight as hp7_weight, \
            hp8.weight as hp8_weight, \
            hp9.weight as hp9_weight, \
            hp10.weight as hp10_weight, \
            hp11.weight as hp11_weight, \
            ap1.weight as ap1_weight, \
            ap2.weight as ap2_weight, \
            ap3.weight as ap3_weight, \
            ap4.weight as ap4_weight, \
            ap5.weight as ap5_weight, \
            ap6.weight as ap6_weight, \
            ap7.weight as ap7_weight, \
            ap8.weight as ap8_weight, \
            ap9.weight as ap9_weight, \
            ap10.weight as ap10_weight, \
            ap11.weight as ap11_weight, \
            Julianday(m.date) - JulianDay(hp1.birthday) as hp1_age, \
            Julianday(m.date) - JulianDay(hp2.birthday) as hp2_age, \
            Julianday(m.date) - JulianDay(hp3.birthday) as hp3_age, \
            Julianday(m.date) - JulianDay(hp4.birthday) as hp4_age, \
            Julianday(m.date) - JulianDay(hp5.birthday) as hp5_age, \
            Julianday(m.date) - JulianDay(hp6.birthday) as hp6_age, \
            Julianday(m.date) - JulianDay(hp7.birthday) as hp7_age, \
            Julianday(m.date) - JulianDay(hp8.birthday) as hp8_age, \
            Julianday(m.date) - JulianDay(hp9.birthday) as hp9_age, \
            Julianday(m.date) - JulianDay(hp10.birthday) as hp10_age, \
            Julianday(m.date) - JulianDay(hp11.birthday) as hp11_age, \
            Julianday(m.date) - JulianDay(ap1.birthday) as ap1_age, \
            Julianday(m.date) - JulianDay(ap2.birthday) as ap2_age, \
            Julianday(m.date) - JulianDay(ap3.birthday) as ap3_age, \
            Julianday(m.date) - JulianDay(ap4.birthday) as ap4_age, \
            Julianday(m.date) - JulianDay(ap5.birthday) as ap5_age, \
            Julianday(m.date) - JulianDay(ap6.birthday) as ap6_age, \
            Julianday(m.date) - JulianDay(ap7.birthday) as ap7_age, \
            Julianday(m.date) - JulianDay(ap8.birthday) as ap8_age, \
            Julianday(m.date) - JulianDay(ap9.birthday) as ap9_age, \
            Julianday(m.date) - JulianDay(ap10.birthday) as ap10_age, \
            Julianday(m.date) - JulianDay(ap11.birthday) as ap11_age, \
            m.home_player_1, \
            m.home_player_2, \
            m.home_player_3, \
            m.home_player_4, \
            m.home_player_5, \
            m.home_player_6, \
            m.home_player_7, \
            m.home_player_8, \
            m.home_player_9, \
            m.home_player_10, \
            m.home_player_11, \
            m.away_player_1, \
            m.away_player_2, \
            m.away_player_3, \
            m.away_player_4, \
            m.away_player_5, \
            m.away_player_6, \
            m.away_player_7, \
            m.away_player_8, \
            m.away_player_9, \
            m.away_player_10, \
            m.away_player_11 \
    FROM Match as m \
    INNER JOIN Country as c \
    ON (c.id = m.country_id) \
    INNER JOIN League as l \
    ON (l.id = m.league_id) \
    INNER JOIN team as ht \
    ON (m.home_team_api_id = ht.team_api_id) \
    INNER JOIN team as at \
    ON (m.away_team_api_id = at.team_api_id) \
    INNER JOIN team_attributes as hta \
    ON (m.home_team_api_id = hta.team_api_id) \
    INNER JOIN team_attributes as ata \
    ON (m.away_team_api_id = ata.team_api_id) \
    INNER JOIN df_date_table as ddt \
    ON (m.id = ddt.match_id AND hta.id = ddt.ht_id AND ata.id = ddt.at_id)
    LEFT JOIN player as hp1 \
    ON (CAST(m.home_player_1 as INT) = hp1.player_api_id) \
    LEFT JOIN player as hp2 \
    ON (CAST(m.home_player_2 as INT) = hp2.player_api_id) \
    LEFT JOIN player as hp3 \
    ON (CAST(m.home_player_3 as INT) = hp3.player_api_id) \
    LEFT JOIN player as hp4 \
    ON (CAST(m.home_player_4 as INT) = hp4.player_api_id) \
    LEFT JOIN player as hp5 \
    ON (CAST(m.home_player_5 as INT) = hp5.player_api_id) \
    LEFT JOIN player as hp6 \
    ON (CAST(m.home_player_6 as INT) = hp6.player_api_id)
    LEFT JOIN player as hp7 \
    ON (CAST(m.home_player_7 as INT) = hp7.player_api_id) \
    LEFT JOIN player as hp8 \
    ON (CAST(m.home_player_8 as INT) = hp8.player_api_id) \
    LEFT JOIN player as hp9 \
    ON (CAST(m.home_player_9 as INT) = hp9.player_api_id) \
    LEFT JOIN player as hp10 \
    ON (CAST(m.home_player_10 as INT) = hp10.player_api_id) \
    LEFT JOIN player as hp11 \
    ON (CAST(m.home_player_11 as INT) = hp11.player_api_id) \
    LEFT JOIN player as ap1 \
    ON (CAST(m.away_player_1 as INT) = ap1.player_api_id) \
    LEFT JOIN player as ap2 \
    ON (CAST(m.away_player_2 as INT) = ap2.player_api_id) \
    LEFT JOIN player as ap3 \
    ON (CAST(m.away_player_3 as INT) = ap3.player_api_id) \
    LEFT JOIN player as ap4 \
    ON (CAST(m.away_player_4 as INT) = ap4.player_api_id) \
    LEFT JOIN player as ap5 \
    ON (CAST(m.away_player_5 as INT) = ap5.player_api_id) \
    LEFT JOIN player as ap6 \
    ON (CAST(m.away_player_6 as INT) = ap6.player_api_id) \
    LEFT JOIN player as ap7 \
    ON (CAST(m.away_player_7 as INT) = ap7.player_api_id) \
    LEFT JOIN player as ap8 \
    ON (CAST(m.away_player_8 as INT) = ap8.player_api_id) \
    LEFT JOIN player as ap9 \
    ON (CAST(m.away_player_9 as INT) = ap9.player_api_id) \
    LEFT JOIN player as ap10 \
    ON (CAST(m.away_player_10 as INT) = ap10.player_api_id) \
    LEFT JOIN player as ap11 \
    ON (CAST(m.away_player_11 as INT) = ap11.player_api_id) """, con)

    # Be sure to close the connection
    con.close()
    
    return df_soccer_data


''' Function that reads in the data based on a SQL query that
    joins relevant tables in the soccer match database. The function
    has input "type", which can take on values "base" (no player info),
    "base_player" (default, base and aggregate player info), and
    "full" (all base and all player data).'''
def read_data(type="base_player"):
    if (type == "base"):
        return base_query()
    elif (type == "base_player"):
        return base_player_query()
    else:
        return full_query()


def set_labels(df):
    n, d = df.shape
    labels = ['']*n
    for i in range(n):
        if (df['home_team_goal'][i] > df['away_team_goal'][i]):
            labels[i] = 'H'
        elif (df['home_team_goal'][i] < df['away_team_goal'][i]):
            labels[i] = 'A'
        elif (df['home_team_goal'][i] == df['away_team_goal'][i]):
            labels[i] = 'D'
        else:
            print('Non-logical situation')
    
    return labels


def player_agg(df):
    height_home = list(df.columns)[92:103]
    height_away = list(df.columns)[103:114]
    weight_home = list(df.columns)[114:125]
    weight_away = list(df.columns)[125:136]
    age_home = list(df.columns)[136:147]
    age_away = list(df.columns)[147:158]
    
    home_mean_height = df[height_home].mean(axis=1)
    away_mean_height = df[height_away].mean(axis=1)
    home_mean_weight = df[weight_home].mean(axis=1)
    away_mean_weight = df[weight_away].mean(axis=1)
    home_mean_age = df[age_home].mean(axis=1)
    away_mean_age = df[age_away].mean(axis=1)

    home_std_height = df[height_home].std(axis=1)
    away_std_height = df[height_away].std(axis=1)
    home_std_weight = df[weight_home].std(axis=1)
    away_std_weight = df[weight_away].std(axis=1)
    home_std_age = df[age_home].std(axis=1)
    away_std_age = df[age_away].std(axis=1)
    
    df_player = pd.concat([home_mean_height, away_mean_height, home_mean_weight,
                away_mean_weight, home_mean_age, away_mean_age, home_std_height,
                away_std_height, home_std_weight, away_std_weight, home_std_age,
                away_std_age], axis = 1)
    
    df_player.columns = ['home_mean_height', 'away_mean_height', 'home_mean_weight', 'away_mean_weight',
                     'home_mean_age', 'away_mean_age', 'home_std_height', 'away_std_height',
                     'home_std_weight', 'away_std_weight', 'home_std_age', 'away_std_age']
    
    return df_player


def dropNan(X):
    nullQ = X.isnull().sum(axis=1)
    dropIdxs = []
    for i in range(X.shape[0]):
        if (X['away_std_age'].isnull().iloc[i] == True):
            dropIdxs.append(i)
        elif (X['away_std_height'].isnull().iloc[i] == True):
            dropIdxs.append(i)
        elif (X['away_std_weight'].isnull().iloc[i] == True):
            dropIdxs.append(i)
        elif (X['home_std_height'].isnull().iloc[i] == True):
            dropIdxs.append(i)
        elif (X['home_std_weight'].isnull().iloc[i] == True):
            dropIdxs.append(i)
        elif (X['home_std_age'].isnull().iloc[i] == True):
            dropIdxs.append(i)
    
    X = X.drop(index = dropIdxs)
    X = X.reset_index()
    
    return X


def create_X(df, labels, df_player=None, type="base_player"):
    X_1 = df.iloc[:,1:8]
    X_2 = df.iloc[:,48:92]
    if (type == "base"):
        df = pd.concat([X_1, X_2], axis=1)
        X = df[X_1.columns.tolist()+
                  [c for c in X_2.columns if ("Class" not in c) or ("DefenderLineClass" in c) \
                               or ("CreationPositioningClass" in c) or ("buildUpPlayPositioningClass" in c) \
                               or ("buildUpPlayDribblingClass" in c)]].copy()
    elif (type == "base_player"):
        df = pd.concat([X_1, X_2], axis=1)
        X = pd.concat([df[X_1.columns.tolist()+
                  [c for c in X_2.columns if ("Class" not in c) or ("DefenderLineClass" in c) \
                               or ("CreationPositioningClass" in c) or ("buildUpPlayPositioningClass" in c) \
                               or ("buildUpPlayDribblingClass" in c)]].copy(), df_player], axis=1)
    else:
        X_3 = df.iloc[:,158:]
        df = pd.concat([X_1, X_2, X_3], axis = 1)
        X = pd.concat([df[X_1.columns.tolist() + X_3.columns.tolist() +
                  [c for c in X_2.columns if ("Class" not in c) or ("DefenderLineClass" in c) \
                               or ("CreationPositioningClass" in c) or ("buildUpPlayPositioningClass" in c) \
                               or ("buildUpPlayDribblingClass" in c)]].copy(), df_player], axis=1)

    X = X.drop("home_buildUpPlayDribbling", axis=1)
    X = X.drop("away_buildUpPlayDribbling", axis=1)
    X = X.drop(['match_date','home_start_date','away_start_date'], axis = 1)
    
    X["label"] = labels 
    
    if (type != "base"):
        X = dropNan(X)
    
    return X


def read_prep_data(type="base_player"):
    df = read_data(type)
    labels = set_labels(df)
    df_player = None
    if (type!="base"):
        df_player = player_agg(df)
    X = create_X(df, labels, df_player, type)
    X = X.drop(columns='index')
    return X