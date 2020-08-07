import mysql.connector
import time
import dbcfg
import hashlib as hl
import HubTbl as ht
import LinkTbl as lt
import SatTbl as st

class ExtractMySQLTbl():
    """
    ExtractMySQLTbl - extract a given MySQL table into its data vault
    components: hubs, links, and satellites.
    """
    def __init__(self, tbl):
        self._tblnm = tbl
        self._links = []
        self._satellites = []
        self._cnct = None

    @property
    def links(self):
        return(self._links)

    @property
    def satellites(self):
        return(self._satellites)

    def startup(self, files=True, justfiles=False):
        if not justfiles:
            self._cnct = mysql.connector.connect(**dbcfg.dbcon)
        if files:
            self._hub.startup()
            for lnk in self.links:
                lnk.startup()
            for sat in self.satellites:
                sat.startup()

    def shutdown(self, files=True, justfiles=False):
        if not justfiles:
            self._cnct.close()
        if files:
            self._hub.shutdown()
            for lnk in self.links:
                lnk.shutdown()
            for sat in self.satellites:
                sat.shutdown()

    def addhub(self, keycolzstr, src):
        self._hub = ht.HubTbl(self._tblnm, src)
        self._hub.addkeycolz(keycolzstr)
        # self._hub.addnonkeycolz(nonkeycolzstr)

    def addlink(self, lnknm, src, keycolzstr, nonkeycolzstr=None):
        lnk = lt.LinkTbl(lnknm, src)
        lnk.addkeycolz(keycolzstr)
        lnk.parenttable = self._tblnm
        if nonkeycolzstr is not None:
            lnk.addnonkeycolz(nonkeycolzstr)
        self.links.append(lnk)

    def addsat(self, src, keycolzstr, nonkeycolzstr):
        sat = st.SatTbl(self._tblnm, src)
        sat.addkeycolz(keycolzstr)
        sat.addnonkeycolz(nonkeycolzstr)
        self.satellites.append(sat)

    def pulldata(self, filtr=""):
        selstr = """
        SELECT *
        FROM {0}
        {1}
        """.format(self._tblnm, filtr)
        print(selstr)
        curs = self._cnct.cursor(buffered=True)
        curs.execute(selstr)
        for ro in curs:
            self._hub.output(curs.column_names, ro)
            for lnk in self.links:
                lnk.output(curs.column_names, ro)
            for sat in self.satellites:
                sat.output(curs.column_names, ro)
        curs.close()

    def pullbypartition(self, selparts, partcol):
        print(selparts)
        curs = self._cnct.cursor(buffered=True)
        curs.execute(selparts)
        for (pcol,) in curs:
            filt = "WHERE {0} = {1}".format(partcol, pcol)
            self.startup(True, True)    # files & only files
            # print(filt)
            self.pulldata(filt)
            self.shutdown(True, True)
            time.sleep(1)   # kludge to keep timestamps distinct

if __name__ == "__main__":
    src = "Win10.laptop.lrg1"

    # emst = ExtractMySQLTbl("strategy_set_member")
    # emst.addhub("StrategySetMemberID", src)
    # emst.addlink("strategy", src, ["StrategyID","StrategySetMemberID"])
    # emst.addlink("strategy_set", src, ["StrategySetID","StrategySetMemberID"])
    # emst.startup()
    # emst.pulldata()
    # emst.shutdown()

    # emst = ExtractMySQLTbl("weighted_strategy_set")
    # emst.addhub("WeightedStrategySetID", src)
    # emst.addlink("strategy_set", src, ["WeightedStrategySetID","StrategySetID"])
    # emst.addsat(src, "WeightedStrategySetID", "WeightSummaryNum")
    # emst.startup(False)
    # partsel = "SELECT DISTINCT StrategySetID FROM weighted_strategy_set"
    # emst.pullbypartition(partsel, "StrategySetID")
    # emst.shutdown(False)

    # emst = ExtractMySQLTbl("weighted_strategy_set_member")
    # emst.addhub("WeightedStrategySetMemberID", src)
    # emst.addlink("weighted_strategy_set", src,
    #              ["WeightedStrategySetID","WeightedStrategySetMemberID"])
    # emst.addlink("strategy_set_member", src,
    #              ["StrategySetMemberID","WeightedStrategySetMemberID"])
    # emst.addsat(src, "WeightedStrategySetMemberID", "WeightNum")
    # emst.startup(False)    # just DB connection
    # # emst.pulldata("WHERE StrategySetMemberID = 77")
    # partsel = "SELECT StrategySetMemberID FROM strategy_set_member"
    # emst.pullbypartition(partsel, "StrategySetMemberID")
    # emst.shutdown(False)    # just DB connection

    # emst = ExtractMySQLTbl("game_stats")
    # emst.addhub("GameId", src)  # Mmmm... should be ID, not Id
    # emst.addsat(src, "GameId", "RunTimeSecs,UpdateTs,FinishFlg")
    # emst.startup()
    # emst.pulldata()
    # emst.shutdown()

    emst = ExtractMySQLTbl("game_results")
    emst.addhub("GameId,PlayerPosNum", src)  # Mmmm... should be ID, not Id
    emst.addsat(src, "GameId,PlayerPosNum", "ScoreCnt,RankNum,WinFlg")
    emst.addlink("weighted_strategy_set", src,
                 ["GameId,PlayerPosNum", "WeightedStrategySetID"])
    emst.addlink("game_stats", src,
                 ["GameId,PlayerPosNum", "GameId"])
    emst.startup()
    emst.pulldata()
    emst.shutdown()

    # emst = ExtractMySQLTbl("agent_value")
    # emst.addhub("WeightedStrategySetID", src)
    # emst.addsat(src, "WeightedStrategySetID", "WinRate,ExecCnt,UpdateEpoch")
    # emst.addlink("weighted_strategy_set", src,
    #              ["WeightedStrategySetID","WeightedStrategySetID"])
    # emst.startup()
    # emst.pulldata()
    # emst.shutdown()

    # emst = ExtractMySQLTbl("strategy")
    # emst.addhub("StrategyID", src)
    # emst.addsat(src, "StrategyID", "StrategyTxt")
    # emst.startup()
    # emst.pulldata()
    # emst.shutdown()

    # emst = ExtractMySQLTbl("strategy_set")
    # emst.addhub("StrategySetID", src)
    # emst.addsat(src, "StrategySetID", "StrategySetTxt")
    # emst.startup()
    # emst.pulldata()
    # emst.shutdown()

    # emst = ExtractMySQLTbl("competition_grp")
    # emst.addhub("CompGrpId", src)
    # emst.addsat(src, "CompGrpId", "CompGrpDescr,SetNum")
    # emst.startup()
    # emst.pulldata()
    # emst.shutdown()
    #
    # emst = ExtractMySQLTbl("competition_grp_member")
    # emst.addhub("CompGrpId", src)
    # emst.addlink("competition_grp", src,
    #              ["CompGrpId","CompGrpId"])
    # emst.addlink("weighted_strategy_set_member", src,
    #              ["WeightedStrategySetMemberID","CompGrpId"])
    # emst.startup()
    # emst.pulldata()
    # emst.shutdown()
