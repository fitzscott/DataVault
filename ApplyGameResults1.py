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
    LEFT OUTER JOIN dvaz1.sch_tgt2.game_results_s tgtgrs
    ON s.grhk = tgtgrs.game_results_hash_key
WHERE s.grhd <> COALESCE(tgtgrs.hash_diff, 'NoMatch')
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
WHERE tgth.game_id IS NULL
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
WHERE tgtl.weighted_strategy_set_hash_key IS NULL
    """,
    # I think this one is wrong. The wgtsum is not a unique index, is it?
    "wssH": """
INSERT INTO DVAZ1.sch_tgt2.weighted_strategy_set_h
SELECT
    h.wsshk,
    CURRENT_TIMESTAMP(),
    h.src,
    h.wgtsum
FROM DVAZ1STG.SCH_LD2.wssH h
    LEFT OUTER JOIN DVAZ1.sch_tgt2.weighted_strategy_set_h tgth
    ON h.wgtsum = tgth.weighted_strategy_set_id
WHERE tgth.weighted_strategy_set_id IS NULL
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
WHERE tgtl.weighted_strategy_set_hash_key IS NULL
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
WHERE tgtl.strategy_set_hash_key IS NULL
    """,
    # and again, won't work as-is
    "ssmH": """
INSERT INTO DVAZ1.sch_tgt2.strategy_set_member_h
SELECT
    h.ssmhk,
    CURRENT_TIMESTAMP(),
    h.src,
    h.strat_stratset
FROM DVAZ1STG.SCH_LD2.ssmH h
    LEFT OUTER JOIN DVAZ1.sch_tgt2.strategy_set_member_h tgth
    ON h.strat_stratset = tgth.strategy_set_member_id
WHERE tgth.strategy_set_member_id IS NULL
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
WHERE tgtl.strategy_hash_key IS NULL
    """
}

# Should parameterize this with format, too, but...
updsql = {
    "grS": """
UPDATE dvaz1.sch_tgt2.game_results_s
SET load_end_dt = mxld.next_ld_dt
FROM
(
SELECT DISTINCT
    gs1.game_results_hash_key,
    gs1.load_dt,
    gs1.load_end_dt,
    gs1.win_flg,
    MIN(gs2.load_dt) next_ld_dt
FROM dvaz1.sch_tgt2.game_results_s gs1
    JOIN dvaz1.sch_tgt2.game_results_s gs2
    ON gs1.game_results_hash_key = gs2.game_results_hash_key
WHERE gs1.load_dt < gs2.load_dt
GROUP BY 1,2,3,4
) mxld
WHERE dvaz1.sch_tgt2.game_results_s.game_results_hash_key = mxld.game_results_hash_key
    AND dvaz1.sch_tgt2.game_results_s.load_dt = mxld.load_dt
    """
}

class ApplyGameResults1():
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
                         "sH": "strategy_h"}
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
        self._cnct.commit()     # necessary?
