-- This is in DVAZ1 / SCH_TGT2.
SELECT
    agent_value_hash_key,
    COUNT(*) cnt,
    COUNT(DISTINCT load_dt) cnt_ld_dt,
    AVG(win_rate) avg_wr,
    SUM(exec_cnt) xcnt,
    MIN(update_epoch) min_epc,
    MAX(update_epoch) max_epc
FROM agent_value_s
GROUP BY 1
HAVING cnt > 6
;

SELECT *
FROM agent_value_s
WHERE agent_value_hash_key = 'aa902ae1cbcbda01f186ab64200050a4'
ORDER BY load_dt
;

-- SCH_TGT1
SELECT *
FROM 
(
SELECT 
    wss.strategy_set_id,
    MAX(avs.win_rate) mx_wr,
    MAX(avs.exec_cnt) mx_xc,
    COUNT(*) cnt
FROM agent_value_h avh
    JOIN agent_value_s avs
    ON avh.agent_value_hash_key = avs.agent_value_hash_key
    JOIN weighted_strategy_set_x_agent_value_l wss_av
    ON avh.agent_value_hash_key = wss_av.agent_value_hash_key
    JOIN weighted_strategy_set_h wss
    ON wss_av.weighted_strategy_set_hash_key = wss.weighted_strategy_set_hash_key
GROUP BY 1
--HAVING mx_xc > 5
) bst
    JOIN
(
SELECT
  wss.strategy_set_id,
  avs.*
FROM agent_value_h avh
    JOIN agent_value_s avs
    ON avh.agent_value_hash_key = avs.agent_value_hash_key
    JOIN weighted_strategy_set_x_agent_value_l wss_av
    ON avh.agent_value_hash_key = wss_av.agent_value_hash_key
    JOIN weighted_strategy_set_h wss
    ON wss_av.weighted_strategy_set_hash_key = wss.weighted_strategy_set_hash_key
WHERE avs.record_src = 'Linux.GCP.01'
) dtl
    ON bst.strategy_set_id = dtl.strategy_set_id
    AND bst.mx_wr = dtl.win_rate
;

DROP TABLE IF EXISTS dvaz1.sch_tgtbiz.strategies_d;

CREATE TABLE dvaz1.sch_tgtbiz.strategies_d
(
                weighted_strategy_set_hash_key CHAR(32) NOT NULL,
                weighted_strategy_set_id BIGINT,
                weight_summary BIGINT,
                strategy_set_id INT,
                strategy_set_txt VARCHAR(1023),
                PRIMARY KEY (weighted_strategy_set_hash_key)
) 
;

INSERT INTO dvaz1.sch_tgtbiz.strategies_d
SELECT
    wssh.weighted_strategy_set_hash_key,
    wssh.weighted_strategy_set_id,
    wsss.weight_summary_num,
    ssh.strategy_set_id,
    sss.strategy_set_txt
FROM weighted_strategy_set_h wssh
    JOIN weighted_strategy_set_s wsss
    ON wssh.weighted_strategy_set_hash_key = wsss.weighted_strategy_set_hash_key
    JOIN strategy_set_x_weighted_strategy_set_l ss_wss
    ON wssh.weighted_strategy_set_hash_key = ss_wss.weighted_strategy_set_hash_key
    JOIN strategy_set_h ssh
    ON ss_wss.strategy_set_hash_key = ssh.strategy_set_hash_key
    JOIN strategy_set_s sss
    ON ssh.strategy_set_hash_key = sss.strategy_set_hash_key
;

/* just yields the agents being tested – 1, 9, & 10 – on the big Win10 laptop */
SELECT
    strategy_set_id,
    --strategy_set_txt,
    COUNT(*) AS cnt,
    COUNT(DISTINCT weighted_strategy_set_id) AS wss_cnt,
   COUNT(DISTINCT weight_summary) AS wgt_cnt
FROM dvaz1.sch_tgtbiz.strategies_d
--WHERE strategy_set_id = 1
GROUP BY 1--,2
HAVING cnt > 1000000
;


DROP TABLE IF EXISTS dvaz1.sch_tgtbiz.agent_value_f
;

CREATE TABLE dvaz1.sch_tgtbiz.agent_value_f
(
    agent_value_hash_key CHAR(32) NOT NULL,
    weighted_strategy_set_hash_key CHAR(32) NOT NULL,
    agent_value_id BIGINT,
    win_rate DECIMAL(16,14),
    exec_cnt INT,
    update_epoch VARCHAR(24),
    PRIMARY KEY (agent_value_hash_key)
)
;

INSERT INTO agent_value_f
SELECT
    avh.agent_value_hash_key,
    wss_av.weighted_strategy_set_hash_key,
    avh.agent_value_id,
    avs.win_rate,
    avs.exec_cnt,
    avs.update_epoch
FROM agent_value_h avh
    JOIN agent_value_s avs
    ON avh.agent_value_hash_key = avs.agent_value_hash_key
    JOIN weighted_strategy_set_x_agent_value_l wss_av
    ON avh.agent_value_hash_key = wss_av.agent_value_hash_key
;

DROP TABLE IF EXISTS dvaz1.sch_tgtbiz.game_f;

CREATE TABLE dvaz1.sch_tgtbiz.game_f
(
    game_results_hash_key CHAR(32) NOT NULL,
    weighted_strategy_set_hash_key CHAR(32) NOT NULL,
    game_id BIGINT,
    run_time_secs FLOAT,
    update_ts TIMESTAMP(0),
    finish_flg CHAR(1),
    player_pos_num TINYINT,
    score_cnt SMALLINT,
    rank_num TINYINT,
    win_flg CHAR(1),
    PRIMARY KEY (game_results_hash_key)
);

INSERT INTO dvaz1.sch_tgtbiz.game_f
SELECT
    grh.game_results_hash_key,
    wss_gr.weighted_strategy_set_hash_key,
    gsh.game_id,
    gss.run_time_secs,
    gss.update_ts,
    gss.finish_flg,
    grh.player_pos_num,
    grs.score_cnt,
    grs.rank_num,
    grs.win_flg
FROM game_results_h grh
    JOIN game_results_s grs
    ON grh.game_results_hash_key = grs.game_results_hash_key
    JOIN game_stats_x_game_results_l gs_gr
    ON grh.game_results_hash_key = gs_gr.game_results_hash_key
    JOIN game_stats_h gsh
    ON gs_gr.game_stats_hash_key = gsh.game_stats_hash_key
    JOIN game_stats_s gss
    ON gsh.game_stats_hash_key = gss.game_stats_hash_key
    JOIN weighted_strategy_set_x_game_results_l wss_gr
    ON grh.game_results_hash_key = wss_gr.game_results_hash_key
;

SELECT
    ssh.strategy_set_id,
    MIN(avs.load_dt) min_av_lddt,
    MAX(avs.load_dt) max_av_lddt,
    COUNT(DISTINCT avs.load_dt) distcnt_av_lddt,
    AVG(avs.win_rate) avg_winrt,
    MIN(avs.exec_cnt) min_xcnt,
    MAX(avs.exec_cnt) max_xcnt,
    MIN(avs.update_epoch) min_epc,
    MAX(avs.update_epoch) max_epc,
    COUNT(DISTINCT wssh.weighted_strategy_set_id) distcnt_wssid,
    COUNT(*) cnt
FROM weighted_strategy_set_h wssh
    JOIN weighted_strategy_set_s wsss
    ON wssh.weighted_strategy_set_hash_key = wsss.weighted_strategy_set_hash_key
    JOIN strategy_set_x_weighted_strategy_set_l ss_wss
    ON wssh.weighted_strategy_set_hash_key = ss_wss.weighted_strategy_set_hash_key
    JOIN strategy_set_h ssh
    ON ss_wss.strategy_set_hash_key = ssh.strategy_set_hash_key
    JOIN weighted_strategy_set_x_agent_value_l wss_av
    ON wssh.weighted_strategy_set_hash_key = wss_av.weighted_strategy_set_hash_key
    JOIN agent_value_h avh
    ON wss_av.agent_value_hash_key = avh.agent_value_hash_key
    JOIN agent_value_s avs
    ON avh.agent_value_hash_key = avs.agent_value_hash_key
GROUP BY 1
ORDER BY cnt DESC
;

SELECT
    ssh.strategy_set_id,
    wssh.weighted_strategy_set_id,
    avs.win_rate,
    avs.exec_cnt,
    RANK() OVER (PARTITION BY ssh.strategy_set_id
                ORDER BY avs.win_rate DESC) win_rank
FROM weighted_strategy_set_h wssh
    JOIN weighted_strategy_set_s wsss
    ON wssh.weighted_strategy_set_hash_key = wsss.weighted_strategy_set_hash_key
    JOIN strategy_set_x_weighted_strategy_set_l ss_wss
    ON wssh.weighted_strategy_set_hash_key = ss_wss.weighted_strategy_set_hash_key
    JOIN strategy_set_h ssh
    ON ss_wss.strategy_set_hash_key = ssh.strategy_set_hash_key
    JOIN weighted_strategy_set_x_agent_value_l wss_av
    ON wssh.weighted_strategy_set_hash_key = wss_av.weighted_strategy_set_hash_key
    JOIN agent_value_h avh
    ON wss_av.agent_value_hash_key = avh.agent_value_hash_key
    JOIN agent_value_s avs
    ON avh.agent_value_hash_key = avs.agent_value_hash_key
WHERE ssh.strategy_set_id IN (1, 9, 10)
    AND exec_cnt > 4
QUALIFY win_rank = 1
;

