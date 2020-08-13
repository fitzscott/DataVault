DROP TABLE IF EXISTS  strategy_x_strategy_set_member_l
; DROP TABLE IF EXISTS  strategy_set_x_strategy_set_member_l
; DROP TABLE IF EXISTS  strategy_set_member_x_weighted_strategy_set_member_l
; DROP TABLE IF EXISTS  weighted_strategy_set_x_weighted_strategy_set_member_l
; DROP TABLE IF EXISTS  weighted_strategy_set_member_x_competition_grp_member_l
; DROP TABLE IF EXISTS  competition_grp_x_competition_grp_member_l
; DROP TABLE IF EXISTS  strategy_set_x_weighted_strategy_set_l
; DROP TABLE IF EXISTS  weighted_strategy_set_x_agent_value_l
; DROP TABLE IF EXISTS  weighted_strategy_set_x_game_results_l
; DROP TABLE IF EXISTS  game_results_x_game_stats_l
;

CREATE TABLE strategy_x_strategy_set_member_l
(
    strategy_x_strategy_set_member_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    strategy_hash_key CHAR(32),
    strategy_set_member_hash_key CHAR(32),
    PRIMARY KEY(strategy_x_strategy_set_member_hash_key)
);

CREATE TABLE strategy_set_x_strategy_set_member_l
(
    strategy_set_x_strategy_set_member_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    strategy_set_hash_key CHAR(32),
    strategy_set_member_hash_key CHAR(32),
    PRIMARY KEY(strategy_set_x_strategy_set_member_hash_key)
);

CREATE TABLE strategy_set_member_x_weighted_strategy_set_member_l
(
    strategy_set_member_x_weighted_strategy_set_member_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    strategy_set_member_hash_key CHAR(32),
    weighted_strategy_set_member_hash_key CHAR(32),
    PRIMARY KEY(strategy_set_member_x_weighted_strategy_set_member_hash_key)
);

CREATE TABLE weighted_strategy_set_x_weighted_strategy_set_member_l
(
    weighted_strategy_set_x_weighted_strategy_set_member_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    weighted_strategy_set_hash_key CHAR(32),
    weighted_strategy_set_member_hash_key CHAR(32),
    PRIMARY KEY(weighted_strategy_set_x_weighted_strategy_set_member_hash_key)
);

CREATE TABLE weighted_strategy_set_member_x_competition_grp_member_l
(
    weighted_strategy_set_member_x_competition_grp_member_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    weighted_strategy_set_member_hash_key CHAR(32),
    competition_grp_member_hash_key CHAR(32),
    PRIMARY KEY(weighted_strategy_set_member_x_competition_grp_member_hash_key)
);

CREATE TABLE competition_grp_x_competition_grp_member_l
(
    competition_grp_member_x_competition_grp_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    competition_grp_member_hash_key CHAR(32),
    competition_grp_hash_key CHAR(32),
    PRIMARY KEY(competition_grp_member_x_competition_grp_hash_key)
);

CREATE TABLE strategy_set_x_weighted_strategy_set_l
(
    strategy_set_x_weighted_strategy_set_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    strategy_set_hash_key CHAR(32),
    weighted_strategy_set_hash_key CHAR(32),
    PRIMARY KEY(strategy_set_x_weighted_strategy_set_hash_key)
);

CREATE TABLE weighted_strategy_set_x_agent_value_l
(
    weighted_strategy_set_x_agent_value_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    weighted_strategy_set_hash_key CHAR(32),
    agent_value_hash_key CHAR(32),
    PRIMARY KEY(weighted_strategy_set_x_agent_value_hash_key)
);

CREATE TABLE weighted_strategy_set_x_game_results_l
(
    weighted_strategy_set_x_game_results_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    weighted_strategy_set_hash_key CHAR(32),
    game_results_hash_key CHAR(32),
    PRIMARY KEY(weighted_strategy_set_x_game_results_hash_key)
);

CREATE TABLE game_stats_x_game_results_l
(
    game_stats_x_game_results_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    game_stats_hash_key CHAR(32),
    game_results_hash_key CHAR(32),
    PRIMARY KEY(game_stats_x_game_results_hash_key)
);
