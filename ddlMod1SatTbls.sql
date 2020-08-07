DROP TABLE IF EXISTS  strategy_s
; DROP TABLE IF EXISTS  strategy_set_s
; DROP TABLE IF EXISTS  weighted_strategy_set_s
; DROP TABLE IF EXISTS  competition_grp_s
; DROP TABLE IF EXISTS  game_results_s
; DROP TABLE IF EXISTS  game_stats_s
; DROP TABLE IF EXISTS  weighted_strategy_set_member_s
; DROP TABLE IF EXISTS  agent_value_s
;

CREATE TABLE strategy_s
(
    strategy_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    load_end_dt TIMESTAMP NULL,
    record_src VARCHAR(40),
    hash_diff CHAR(32),
    strategy_txt VARCHAR(80),
    PRIMARY KEY(strategy_hash_key, load_dt)
);

CREATE TABLE strategy_set_s
(
    strategy_set_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    load_end_dt TIMESTAMP NULL,
    record_src VARCHAR(40),
    hash_diff CHAR(32),
    strategy_set_txt VARCHAR(512),
    PRIMARY KEY(strategy_set_hash_key, load_dt)
);

CREATE TABLE weighted_strategy_set_s
(
    weighted_strategy_set_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    load_end_dt TIMESTAMP NULL,
    record_src VARCHAR(40),
    hash_diff CHAR(32),
    weight_summary_num VARCHAR(20),
    PRIMARY KEY(weighted_strategy_set_hash_key, load_dt)
);

CREATE TABLE competition_grp_s
(
    competition_grp_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    load_end_dt TIMESTAMP NULL,
    record_src VARCHAR(40),
    hash_diff CHAR(32),
    set_num VARCHAR(10),
    PRIMARY KEY(competition_grp_hash_key, load_dt)
);

CREATE TABLE game_results_s
(
    game_results_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    load_end_dt TIMESTAMP NULL,
    record_src VARCHAR(40),
    hash_diff CHAR(32),
    score_cnt VARCHAR(4) NOT NULL,
    rank_num VARCHAR(2) NULL,
    win_flg CHAR(1) NULL,
    PRIMARY KEY(game_results_hash_key, load_dt)
);

CREATE TABLE game_stats_s
(
    game_stats_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    load_end_dt TIMESTAMP NULL,
    record_src VARCHAR(40),
    hash_diff CHAR(32),
    run_time_secs VARCHAR(20) NULL,
    update_ts VARCHAR(80) NULL,
    finish_flg CHAR(1) NULL,
    PRIMARY KEY(game_stats_hash_key, load_dt)
);

CREATE TABLE weighted_strategy_set_member_s
(
    weighted_strategy_set_member_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    load_end_dt TIMESTAMP NULL,
    record_src VARCHAR(40),
    hash_diff CHAR(32),
    weight_num VARCHAR(20),
    PRIMARY KEY(weighted_strategy_set_member_hash_key, load_dt)
);

CREATE TABLE agent_value_s
(
    agent_value_hash_key CHAR(32)  NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    load_end_dt TIMESTAMP NULL,
    hash_diff CHAR(32),
    record_src VARCHAR(40),
    win_rate VARCHAR(20) NULL,
    exec_cnt VARCHAR(12) NULL,
    update_epoch VARCHAR(24) NULL,
    PRIMARY KEY(agent_value_hash_key, load_dt)
);


