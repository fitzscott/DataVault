USE DATABASE DVAZ1STG;
USE SCHEMA SCH_LD1;

DROP TABLE IF EXISTS strategy_h
; DROP TABLE IF EXISTS strategy_set_h
; DROP TABLE IF EXISTS strategy_set_member_h
; DROP TABLE IF EXISTS weighted_strategy_set_h
; DROP TABLE IF EXISTS weighted_strategy_set_member_h
; DROP TABLE IF EXISTS game_results_h
; DROP TABLE IF EXISTS game_stats_h
; DROP TABLE IF EXISTS competition_grp_h
; DROP TABLE IF EXISTS competition_grp_member_h
; DROP TABLE IF EXISTS agent_value_h
; DROP TABLE IF EXISTS  strategy_x_strategy_set_member_l
; DROP TABLE IF EXISTS  strategy_set_x_strategy_set_member_l
; DROP TABLE IF EXISTS  strategy_set_member_x_weighted_strategy_set_member_l
; DROP TABLE IF EXISTS  weighted_strategy_set_x_weighted_strategy_set_member_l
; DROP TABLE IF EXISTS  weighted_strategy_set_member_x_competition_grp_member_l
; DROP TABLE IF EXISTS  competition_grp_x_competition_grp_member_l
; DROP TABLE IF EXISTS  strategy_set_x_weighted_strategy_set_l
; DROP TABLE IF EXISTS  weighted_strategy_set_x_agent_value_l
; DROP TABLE IF EXISTS  weighted_strategy_set_x_game_results_l
; DROP TABLE IF EXISTS  game_results_x_game_stats_l
; DROP TABLE IF EXISTS  game_stats_x_game_results_l
; DROP TABLE IF EXISTS  strategy_s
; DROP TABLE IF EXISTS  strategy_set_s
; DROP TABLE IF EXISTS  weighted_strategy_set_s
; DROP TABLE IF EXISTS  competition_grp_s
; DROP TABLE IF EXISTS  game_results_s
; DROP TABLE IF EXISTS  game_stats_s
; DROP TABLE IF EXISTS  weighted_strategy_set_member_s
; DROP TABLE IF EXISTS  agent_value_s
;

CREATE TABLE dvaz1stg.sch_ld1.strategy_h
(
    record_src VARCHAR(40),
    strategy_hash_key CHAR(32) NOT NULL,
    strategy_id BIGINT NOT NULL,
    PRIMARY KEY(strategy_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.strategy_set_h
(
    record_src VARCHAR(40),
    strategy_set_hash_key CHAR(32) NOT NULL,
    strategy_set_id BIGINT NOT NULL,
    PRIMARY KEY(strategy_set_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.strategy_set_member_h
(
    record_src VARCHAR(40),
    strategy_set_member_hash_key CHAR(32) NOT NULL,
    strategy_set_member_id BIGINT NOT NULL,
    PRIMARY KEY(strategy_set_member_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.weighted_strategy_set_h
(
    record_src VARCHAR(40),
    weighted_strategy_set_hash_key CHAR(32) NOT NULL,
    weighted_strategy_set_id BIGINT NOT NULL,
    PRIMARY KEY(weighted_strategy_set_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.weighted_strategy_set_member_h
(
    record_src VARCHAR(40),
    weighted_strategy_set_member_hash_key CHAR(32) NOT NULL,
    weighted_strategy_set_member_id BIGINT NOT NULL,
    PRIMARY KEY(weighted_strategy_set_member_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.game_results_h
(
    record_src VARCHAR(40),
    game_results_hash_key CHAR(32) NOT NULL,
    game_id BIGINT NOT NULL,
    player_pos_num SMALLINT NOT NULL,
    PRIMARY KEY(game_results_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.game_stats_h
(
    record_src VARCHAR(40),
    game_stats_hash_key CHAR(32) NOT NULL,
    game_id BIGINT NOT NULL,
    PRIMARY KEY(game_stats_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.competition_grp_h
(
    record_src VARCHAR(40),
    competition_grp_hash_key CHAR(32) NOT NULL,
    competition_grp_id BIGINT NOT NULL,
    PRIMARY KEY(competition_grp_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.competition_grp_member_h
(
    record_src VARCHAR(40),
    competition_grp_member_hash_key CHAR(32) NOT NULL,
    competition_grp_member_id BIGINT NOT NULL,
    PRIMARY KEY(competition_grp_member_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.agent_value_h
(
    record_src VARCHAR(40),
    agent_value_hash_key CHAR(32) NOT NULL,
    agent_value_id BIGINT NOT NULL,
    PRIMARY KEY(agent_value_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.strategy_x_strategy_set_member_l
(
    record_src VARCHAR(40),
    strategy_x_strategy_set_member_hash_key CHAR(32) NOT NULL,
    strategy_hash_key CHAR(32),
    strategy_set_member_hash_key CHAR(32),
    PRIMARY KEY(strategy_x_strategy_set_member_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.strategy_set_x_strategy_set_member_l
(
    record_src VARCHAR(40),
    strategy_set_x_strategy_set_member_hash_key CHAR(32) NOT NULL,
    strategy_set_hash_key CHAR(32),
    strategy_set_member_hash_key CHAR(32),
    PRIMARY KEY(strategy_set_x_strategy_set_member_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.strategy_set_member_x_weighted_strategy_set_member_l
(
    record_src VARCHAR(40),
    strategy_set_member_x_weighted_strategy_set_member_hash_key CHAR(32) NOT NULL,
    strategy_set_member_hash_key CHAR(32),
    weighted_strategy_set_member_hash_key CHAR(32),
    PRIMARY KEY(strategy_set_member_x_weighted_strategy_set_member_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.weighted_strategy_set_x_weighted_strategy_set_member_l
(
    record_src VARCHAR(40),
    weighted_strategy_set_x_weighted_strategy_set_member_hash_key CHAR(32) NOT NULL,
    weighted_strategy_set_hash_key CHAR(32),
    weighted_strategy_set_member_hash_key CHAR(32),
    PRIMARY KEY(weighted_strategy_set_x_weighted_strategy_set_member_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.weighted_strategy_set_member_x_competition_grp_member_l
(
    record_src VARCHAR(40),
    weighted_strategy_set_member_x_competition_grp_member_hash_key CHAR(32) NOT NULL,
    weighted_strategy_set_member_hash_key CHAR(32),
    competition_grp_member_hash_key CHAR(32),
    PRIMARY KEY(weighted_strategy_set_member_x_competition_grp_member_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.competition_grp_x_competition_grp_member_l
(
    record_src VARCHAR(40),
    competition_grp_member_x_competition_grp_hash_key CHAR(32) NOT NULL,
    competition_grp_member_hash_key CHAR(32),
    competition_grp_hash_key CHAR(32),
    PRIMARY KEY(competition_grp_member_x_competition_grp_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.strategy_set_x_weighted_strategy_set_l
(
    record_src VARCHAR(40),
    strategy_set_x_weighted_strategy_set_hash_key CHAR(32) NOT NULL,
    strategy_set_hash_key CHAR(32),
    weighted_strategy_set_hash_key CHAR(32),
    PRIMARY KEY(strategy_set_x_weighted_strategy_set_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.weighted_strategy_set_x_agent_value_l
(
    record_src VARCHAR(40),
    weighted_strategy_set_x_agent_value_hash_key CHAR(32) NOT NULL,
    weighted_strategy_set_hash_key CHAR(32),
    agent_value_hash_key CHAR(32),
    PRIMARY KEY(weighted_strategy_set_x_agent_value_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.weighted_strategy_set_x_game_results_l
(
    record_src VARCHAR(40),
    weighted_strategy_set_x_game_results_hash_key CHAR(32) NOT NULL,
    weighted_strategy_set_hash_key CHAR(32),
    game_results_hash_key CHAR(32),
    PRIMARY KEY(weighted_strategy_set_x_game_results_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.game_stats_x_game_results_l
(
    record_src VARCHAR(40),
    game_stats_x_game_results_hash_key CHAR(32) NOT NULL,
    game_stats_hash_key CHAR(32),
    game_results_hash_key CHAR(32),
    PRIMARY KEY(game_stats_x_game_results_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.strategy_s
(
    record_src VARCHAR(40),
    strategy_hash_key CHAR(32) NOT NULL,
    hash_diff CHAR(32),
    strategy_txt VARCHAR(80),
    PRIMARY KEY(strategy_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.strategy_set_s
(
    record_src VARCHAR(40),
    strategy_set_hash_key CHAR(32) NOT NULL,
    hash_diff CHAR(32),
    strategy_set_txt VARCHAR(512),
    PRIMARY KEY(strategy_set_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.weighted_strategy_set_s
(
    record_src VARCHAR(40),
    weighted_strategy_set_hash_key CHAR(32) NOT NULL,
    hash_diff CHAR(32),
    weight_summary_num VARCHAR(20),
    PRIMARY KEY(weighted_strategy_set_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.competition_grp_s
(
    record_src VARCHAR(40),
    competition_grp_hash_key CHAR(32) NOT NULL,
    hash_diff CHAR(32),
    set_num VARCHAR(10),
    PRIMARY KEY(competition_grp_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.game_results_s
(
    record_src VARCHAR(40),
    game_results_hash_key CHAR(32) NOT NULL,
    hash_diff CHAR(32),
    score_cnt VARCHAR(4) NOT NULL,
    rank_num VARCHAR(2) NULL,
    win_flg CHAR(1) NULL,
    PRIMARY KEY(game_results_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.game_stats_s
(
    record_src VARCHAR(40),
    game_stats_hash_key CHAR(32) NOT NULL,
    hash_diff CHAR(32),
    run_time_secs VARCHAR(20) NULL,
    update_ts VARCHAR(80) NULL,
    finish_flg CHAR(1) NULL,
    PRIMARY KEY(game_stats_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.weighted_strategy_set_member_s
(
    record_src VARCHAR(40),
    weighted_strategy_set_member_hash_key CHAR(32) NOT NULL,
    hash_diff CHAR(32),
    weight_num VARCHAR(20),
    PRIMARY KEY(weighted_strategy_set_member_hash_key)
);


CREATE TABLE dvaz1stg.sch_ld1.agent_value_s
(
    record_src VARCHAR(40),
    agent_value_hash_key CHAR(32) NOT NULL,
    hash_diff CHAR(32),
    win_rate VARCHAR(20) NULL,
    exec_cnt VARCHAR(12) NULL,
    update_epoch VARCHAR(24) NULL,
    PRIMARY KEY(agent_value_hash_key)
);

