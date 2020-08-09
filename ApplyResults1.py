import snoflkcon


# I would prefer to do this via metadata, since the tables are all structured
# the same, but my staging tables have different column names (lazy - bad),
# so it'd be a little trickier, anyway. Figure out the basics now & do the
# later iterations with metadata, maybe.
# Might not be smart, either, to use the current timestamp function. Meh.
inssql = {
    "grS": """
INSERT INTO dvaz1.sch_tgt2.game_results_s
SELECT DISTINCT
    s.grhk,
    CURRENT_TIMESTAMP(),
    NULL,
    s.src,
    s.grhd,
    s.score,
    s.srank,
    s.winloss
FROM DVAZ1STG.SCH_LD2.grS s
    LEFT OUTER JOIN dvaz1.sch_tgt2.game_results_s tgts
    ON s.grhk = tgts.game_results_hash_key
    AND s.src = tgts.record_src
WHERE s.grhd <> COALESCE(tgts.hash_diff, 'NoMatch')
    AND tgts.load_end_dt IS NULL
    """,
    "grH": """
INSERT INTO DVAZ1.sch_tgt2.game_results_h
SELECT
    h.grhk,
    CURRENT_TIMESTAMP(),
    h.src,
    h.gameid,
    h.posnum
FROM DVAZ1STG.SCH_LD2.grH h
    LEFT OUTER JOIN DVAZ1.sch_tgt2.game_results_h tgth
    /* join on biz keys, not hash keys */
    ON h.gameid = tgth.game_id
    AND h.posnum = tgth.player_pos_num
    AND h.src = tgth.record_src
WHERE tgth.game_id IS NULL
    AND tgth.player_pos_num IS NULL
    """,
    "wssLgr": """
INSERT INTO DVAZ1.sch_tgt2.weighted_strategy_set_x_game_results_l
SELECT
    l.wssLgrhk,
    CURRENT_TIMESTAMP(),
    l.src,
    l.wsshk,
    l.grhk
FROM DVAZ1STG.SCH_LD2.wssLgr l
    LEFT OUTER JOIN DVAZ1.sch_tgt2.weighted_strategy_set_x_game_results_l tgtl
    ON l.wsshk = tgtl.weighted_strategy_set_hash_key
    AND l.grhk = tgtl.game_results_hash_key
    AND l.src = tgtl.record_src
WHERE tgtl.weighted_strategy_set_hash_key IS NULL
    AND tgtl.game_results_hash_key IS NULL
    """,
    # I think this one is wrong. The wgtsum is not a unique index, is it?
    "wssH": """
INSERT INTO DVAZ1.sch_tgt2.weighted_strategy_set_h
SELECT
    h.wsshk,
    CURRENT_TIMESTAMP(),
    h.src,
    h.stratset,
    h.wgtsum
FROM DVAZ1STG.SCH_LD2.wssH h
    LEFT OUTER JOIN DVAZ1.sch_tgt2.weighted_strategy_set_h tgth
    ON h.stratset = tgth.strategy_set_id
    AND h.wgtsum = tgth.weight_num
    AND h.src = tgth.record_src
WHERE tgth.strategy_set_id IS NULL
    AND tgth.weight_num IS NULL
    """,
    "ssLwss": """
INSERT INTO DVAZ1.sch_tgt2.strategy_set_x_weighted_strategy_set_l
SELECT
    l.ssLwsshk,
    CURRENT_TIMESTAMP(),
    l.src,
    l.sshk,
    l.wsshk
FROM DVAZ1STG.SCH_LD2.ssLwss l
    LEFT OUTER JOIN DVAZ1.sch_tgt2.strategy_set_x_weighted_strategy_set_l tgtl
    ON l.sshk = tgtl.strategy_set_hash_key
    AND l.wsshk = tgtl.weighted_strategy_set_hash_key
    AND l.src = tgtl.record_src
WHERE tgtl.strategy_set_hash_key IS NULL
    AND tgtl.weighted_strategy_set_hash_key IS NULL
    """,
    # This won't work, either, but for a different reason:  The data type on
    # the strategy set ID is numeric.
    "ssH": """
INSERT INTO DVAZ1.sch_tgt2.strategy_set_h
SELECT
    h.sshk,
    CURRENT_TIMESTAMP(),
    h.src,
    h.stratset
FROM DVAZ1STG.SCH_LD2.ssH h
    LEFT OUTER JOIN DVAZ1.sch_tgt2.strategy_set_h tgth
    ON h.stratset = tgth.strategy_set_id
    AND h.src = tgth.record_src
WHERE tgth.strategy_set_id IS NULL
    """,
    "ssLssm": """
INSERT INTO DVAZ1.sch_tgt2.strategy_set_x_strategy_set_member_l
SELECT
    l.ssLssmhk,
    CURRENT_TIMESTAMP(),
    l.src,
    l.sshk,
    l.ssmhk
FROM DVAZ1STG.SCH_LD2.ssLssm l
    LEFT OUTER JOIN DVAZ1.sch_tgt2.strategy_set_x_strategy_set_member_l tgtl
    ON l.sshk = tgtl.strategy_set_hash_key
    AND l.ssmhk = tgtl.strategy_set_member_hash_key
    AND l.src = tgtl.record_src
WHERE tgtl.strategy_set_hash_key IS NULL
    AND tgtl.strategy_set_member_hash_key IS NULL
    """,
    "ssmH": """
INSERT INTO DVAZ1.sch_tgt2.strategy_set_member_h
SELECT
    h.ssmhk,
    CURRENT_TIMESTAMP(),
    h.src,
    h.strat,
    h.stratset
FROM DVAZ1STG.SCH_LD2.ssmH h
    LEFT OUTER JOIN DVAZ1.sch_tgt2.strategy_set_member_h tgth
    ON h.strat = tgth.strategy_set_id
    AND h.stratset = tgth.strategy_id
    AND h.src = tgth.record_src
WHERE tgth.strategy_set_id IS NULL
    AND tgth.strategy_id IS NULL
    """,
    "sH": """
INSERT INTO DVAZ1.sch_tgt2.strategy_h
SELECT
    h.shk,
    CURRENT_TIMESTAMP(),
    h.src,
    h.strat
FROM DVAZ1STG.SCH_LD2.sH h
    LEFT OUTER JOIN DVAZ1.sch_tgt2.strategy_h tgth
    ON h.strat = tgth.strategy_id
    AND h.src = tgth.record_src
WHERE tgth.strategy_id IS NULL
    """,
    "sLssm": """
INSERT INTO DVAZ1.sch_tgt2.strategy_x_strategy_set_member_l
SELECT
    l.sLssmhk,
    CURRENT_TIMESTAMP(),
    l.src,
    l.shk,
    l.ssmhk
FROM DVAZ1STG.SCH_LD2.sLssm l
    LEFT OUTER JOIN DVAZ1.sch_tgt2.strategy_x_strategy_set_member_l tgtl
    ON l.shk = tgtl.strategy_hash_key
    AND l.ssmhk = tgtl.strategy_set_member_hash_key
    AND l.src = tgtl.record_src
WHERE tgtl.strategy_hash_key IS NULL
    AND tgtl.strategy_set_member_hash_key IS NULL
    """,
    "avH": """
INSERT INTO DVAZ1.sch_tgt2.agent_value_h
SELECT
    h.avhk,
    CURRENT_TIMESTAMP(),
    h.src,
    h.stratset,
    h.wgtnum
FROM DVAZ1STG.SCH_LD2.avH h
    LEFT OUTER JOIN DVAZ1.sch_tgt2.agent_value_h tgth
    ON h.stratset = tgth.strategy_set_id 
    AND h.wgtnum = tgth.weight_num
    AND h.src = tgth.record_src
WHERE tgth.strategy_set_id IS NULL
    AND tgth.weight_num IS NULL
    """,
    "avS": """
INSERT INTO dvaz1.sch_tgt2.agent_value_s
SELECT DISTINCT
    s.avhk,
    CURRENT_TIMESTAMP(),
    NULL,
    s.src,
    s.avhd,
    s.winrt,
    s.execcnt,
    s.updepc
FROM DVAZ1STG.SCH_LD2.avS s
    LEFT OUTER JOIN dvaz1.sch_tgt2.agent_value_s tgts
    ON s.avhk = tgts.agent_value_hash_key
    AND s.src = tgts.record_src
WHERE s.avhd <> COALESCE(tgts.hash_diff, 'NoMatch')
    AND tgts.load_end_dt IS NULL
    """,
    "wssLav": """
INSERT INTO DVAZ1.sch_tgt2.weighted_strategy_set_x_agent_value_l
SELECT
    l.wssLavhk,
    CURRENT_TIMESTAMP(),
    l.src,
    l.wsshk,
    l.avhk
FROM DVAZ1STG.SCH_LD2.wssLav l
    LEFT OUTER JOIN DVAZ1.sch_tgt2.weighted_strategy_set_x_agent_value_l tgtl
    ON l.wsshk = tgtl.weighted_strategy_set_hash_key
    AND l.avhk = tgtl.agent_value_hash_key
    AND l.src = tgtl.record_src
WHERE tgtl.weighted_strategy_set_hash_key IS NULL
    AND tgtl.agent_value_hash_key IS NULL
    """
}

# Should parameterize this with format, too, but...
updsql = {
    "grS": """
UPDATE dvaz1.sch_tgt2.game_results_s
SET load_end_dt = mxld.next_ld_dt
FROM
(
SELECT
    gs1.game_results_hash_key,
    gs1.load_dt,
    MIN(gs2.load_dt) next_ld_dt
FROM dvaz1.sch_tgt2.game_results_s gs1
    JOIN dvaz1.sch_tgt2.game_results_s gs2
    ON gs1.game_results_hash_key = gs2.game_results_hash_key
WHERE gs1.load_dt < gs2.load_dt
    AND gs1.load_end_dt IS NULL
GROUP BY 1,2
) mxld
WHERE dvaz1.sch_tgt2.game_results_s.game_results_hash_key = mxld.game_results_hash_key
    AND dvaz1.sch_tgt2.game_results_s.load_dt = mxld.load_dt
    """,
    "avS": """
UPDATE dvaz1.sch_tgt2.agent_value_s
SET load_end_dt = mxld.next_ld_dt
FROM
(
SELECT
    av1.agent_value_hash_key,
    av1.load_dt,
    MIN(av2.load_dt) next_ld_dt
FROM dvaz1.sch_tgt2.agent_value_s av1
    JOIN dvaz1.sch_tgt2.agent_value_s av2
    ON av1.agent_value_hash_key = av2.agent_value_hash_key
WHERE av1.load_dt < av2.load_dt
    AND av1.load_end_dt IS NULL
GROUP BY 1,2
) mxld
WHERE dvaz1.sch_tgt2.agent_value_s.agent_value_hash_key = mxld.agent_value_hash_key
    AND dvaz1.sch_tgt2.agent_value_s.load_dt = mxld.load_dt
    """
}

diagsql = {
    "wssH": """
INSERT INTO DVAZ1STG.DIAG2.wssH_diag
SELECT
    h.wsshk,
    CURRENT_TIMESTAMP(),
    h.src,
    h.stratset,
    h.wgtsum
FROM DVAZ1STG.SCH_LD2.wssH h
    LEFT OUTER JOIN DVAZ1.sch_tgt2.weighted_strategy_set_h tgth
    ON h.stratset = tgth.strategy_set_id
    AND h.wgtsum = tgth.weight_num
    AND h.src = tgth.record_src
WHERE (tgth.strategy_set_id IS NOT NULL
    OR tgth.weight_num IS NULL)
    """
}

class ApplyResults1():
    """
    Take staged files & apply them to Snowflake staging tables.
    Then apply changes in staging tables to final targets.
    """
    def __init__(self):
        self._str2tbl = {"grH": "game_results_h", "grS": "game_results_s",
                         "wssLgr": "weighted_strategy_set_x_game_results_l",
                         "wssH": "weighted_strategy_set_h", "ssLwss":
                             "strategy_set_x_weighted_strategy_set_l",
                         "ssH": "strategy_set_h", "ssLssm":
                             "strategy_set_x_strategy_set_member_l",
                         "ssmH": "strategy_set_member_h",
                         "sLssm": "strategy_x_strategy_set_member_l",
                         "sH": "strategy_h", "avH": "agent_value_h",
                         "avS": "agent_value_s",
                         "wssLav": "weighted_strategy_set_x_agent_value_l"}
        self._schema = "SCH_LD2"
        self._cnct = snoflkcon.get_connection()
        self._curs = self._cnct.cursor()
        self._curs.execute("USE SCHEMA " + self._schema)

    def processfile(self, flnm, stgtbl):
        self._flnm = flnm
        self._stgtbl = stgtbl
        assert(stgtbl in self._str2tbl.keys())
        putcmd = r"""
        PUT file://{0} @%{1}
        """.format(flnm, stgtbl)
        print(putcmd)
        self._curs.execute(putcmd)
        self._curs.execute("DELETE FROM {0}".format(stgtbl))
        cpcmd = r"""
        COPY INTO {0}
        FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|')
        """.format(stgtbl)
        print(cpcmd)
        self._curs.execute(cpcmd)
        print("loading " + self._str2tbl[stgtbl])
        print(inssql[stgtbl])
        self._curs.execute(inssql[stgtbl])
        if stgtbl in updsql.keys():
            print(updsql[stgtbl])
            self._curs.execute(updsql[stgtbl])
        if stgtbl in diagsql.keys():
            print(diagsql[stgtbl])
            self._curs.execute(diagsql[stgtbl])
        self._cnct.commit()     # necessary?
