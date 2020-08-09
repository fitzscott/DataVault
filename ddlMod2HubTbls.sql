DROP TABLE IF EXISTS strategy_h
; DROP TABLE IF EXISTS strategy_set_h
; DROP TABLE IF EXISTS strategy_set_member_h
; DROP TABLE IF EXISTS weighted_strategy_set_h
; DROP TABLE IF EXISTS game_results_h
;

CREATE TABLE strategy_h
(
    strategy_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    strategy_id VARCHAR(50) NOT NULL,
    PRIMARY KEY(strategy_hash_key)
);

CREATE TABLE strategy_set_h
(
    strategy_set_hash_key CHAR(32)  NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    strategy_set_id VARCHAR(1023) NOT NULL,
    PRIMARY KEY(strategy_set_hash_key)
);

CREATE TABLE strategy_set_member_h
(
    strategy_set_member_hash_key CHAR(32)  NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    strategy_id VARCHAR(50) NOT NULL,
    strategy_set_id VARCHAR(1023) NOT NULL,
    PRIMARY KEY(strategy_set_member_hash_key)
);

CREATE TABLE weighted_strategy_set_h
(
    weighted_strategy_set_hash_key CHAR(32)  NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    strategy_set_id VARCHAR(1023) NOT NULL,
    weight_num BIGINT NOT NULL,
    PRIMARY KEY(weighted_strategy_set_hash_key)
);

CREATE TABLE game_results_h
(
    game_results_hash_key CHAR(32)  NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    game_id BIGINT NOT NULL,
    player_pos_num SMALLINT NOT NULL,
    PRIMARY KEY(game_results_hash_key)
);


