USE DATABASE DVAZ1STG;
USE SCHEMA DIAG2;

CREATE TABLE wssH_diag
(
    weighted_strategy_set_hash_key CHAR(32)  NOT NULL,
    load_dt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    record_src VARCHAR(40),
    strategy_set_id VARCHAR(1023) NOT NULL,
    weight_num BIGINT NOT NULL
);
