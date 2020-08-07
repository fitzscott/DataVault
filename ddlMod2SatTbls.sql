DROP TABLE IF EXISTS  game_results_s
;


CREATE TABLE game_results_s
(
    game_results_hash_key CHAR(32) NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    load_end_dt TIMESTAMP NULL,
    record_src VARCHAR(40),
    hash_diff CHAR(32),
    score_cnt SMALLINT NOT NULL,
    rank_num TINYINT NULL,
    win_flg CHAR(1) NULL,
    PRIMARY KEY(game_results_hash_key, load_dt)
);

