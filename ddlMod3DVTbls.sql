USE DATABASE DVAZ1;
USE SCHEMA SCH_TGT2;

DROP TABLE IF EXISTS agent_value_h
; DROP TABLE IF EXISTS  weighted_strategy_set_x_agent_value_l
; DROP TABLE IF EXISTS  agent_value_s
;


CREATE TABLE agent_value_h
(
    agent_value_hash_key CHAR(32)  NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    strategy_set_id VARCHAR(1023) NOT NULL,
    weight_num BIGINT NOT NULL,
    PRIMARY KEY(agent_value_hash_key)
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

CREATE TABLE agent_value_s
(
    agent_value_hash_key CHAR(32)  NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    load_end_dt TIMESTAMP NULL,
    record_src VARCHAR(40),
    hash_diff CHAR(32),
    win_rate VARCHAR(30) NULL,
    exec_cnt VARCHAR(12) NULL,
    update_epoch VARCHAR(24) NULL,
    PRIMARY KEY(agent_value_hash_key, load_dt)
);

