import mysql.connector
import time
import dbcfg
import hashlib as hl
import HubTbl as ht
import LinkTbl as lt
import SatTbl as st
import sys
import os
import LoadSnowflakeTbl as lst
import DVInsUpdCre8Stmts as dvius


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
        self._snflk = None
        self._stgdb = "dvaz1stg"
        self._stgsch = "sch_ld1"

    @property
    def links(self):
        return(self._links)

    @property
    def satellites(self):
        return(self._satellites)

    def openconnections(self, files=True, justfiles=False):
        self._cnct = mysql.connector.connect(**dbcfg.dbcon)
        self._snflk = lst.LoadSnowflakeTbl()
        self._snflk.connect()

    def closeconnections(self):
        self._cnct.close()
        self._snflk.disconnect()

    def openfiles(self):
        self._hub.startup()
        for lnk in self.links:
            lnk.startup()
        for sat in self.satellites:
            sat.startup()

    def closefiles(self):
        self._hub.shutdown()
        for lnk in self.links:
            lnk.shutdown()
        for sat in self.satellites:
            sat.shutdown()

    def deletefiles(self):
        self._hub.deletefile()
        for lnk in self.links:
            lnk.deletefile()
        for sat in self.satellites:
            sat.deletefile()

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

    def cleartbl(self, curs, tblnm):
        tbl = tblnm.lower()
        # delstmt = """
        # DELETE FROM {0}.{1}.{2}
        # """.format(self._stgdb, self._stgsch, tbl)
        # print(delstmt)
        # Some issues came up with residual data staged for the staging
        # tables, so we'll drop & re-create, instead.
        curs.execute("DROP TABLE IF EXISTS dvaz1stg.sch_ld1." + tbl)
        self._snflk.connection.commit()
        curs.execute(dvius.cr8stmts[tbl])
        self._snflk.connection.commit()

    def cleartargets(self):
        curs = self._snflk.connection.cursor()
        hubtbl = self._hub.tablename()
        self.cleartbl(curs, hubtbl)
        for lnk in self._links:
            lnktbl = lnk.tablename()
            self.cleartbl(curs, lnktbl)
        for sat in self._satellites:
            sattbl = sat.tablename()
            self.cleartbl(curs, sattbl)
        self._snflk.connection.commit()
        curs.close()

    def runstmt(self, tblobj, stmtdict):
        tblnm = tblobj.tablename().lower()
        # print("Running DML on " + tblnm)
        assert (tblnm in stmtdict.keys())
        print(stmtdict[tblnm])
        curs = self._snflk.connection.cursor()
        curs.execute(stmtdict[tblnm])
        self._snflk.connection.commit()

    def runinsert(self, tblobj):
        self.runstmt(tblobj, dvius.insstmts)

    def runupdate(self, tblobj):
        self.runstmt(tblobj, dvius.updstmts)

    def pulldata(self, filtr=""):
        self.openfiles()
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
        self.closefiles()
        # now save to Snowflake
        self.cleartargets()
        # clean up files after they're loaded - later
        self._snflk.loadfromfile(self._hub.filename())
        self.runinsert(self._hub)
        for lnk in self.links:
            self._snflk.loadfromfile(lnk.filename())
            self.runinsert(lnk)
        for sat in self.satellites:
            self._snflk.loadfromfile(sat.filename())
            self.runinsert(sat)
            self.runupdate(sat)
        self.deletefiles()

    def pullbypartition(self, selparts, partcol):
        print(selparts)
        curs = self._cnct.cursor(buffered=True)
        curs.execute(selparts)
        for (pcol,) in curs:
            filt = "WHERE {0} = {1}".format(partcol, pcol)
            # print(filt)
            self.pulldata(filt)
            # self.shutdown(True, True)
            time.sleep(1)   # kludge to keep timestamps distinct

def processall(src):
    emst = ExtractMySQLTbl("strategy_set_member")
    emst.addhub("StrategySetMemberID", src)
    emst.addlink("strategy", src, ["StrategyID","StrategySetMemberID"])
    emst.addlink("strategy_set", src, ["StrategySetID","StrategySetMemberID"])
    emst.openconnections()
    emst.pulldata()
    emst.closeconnections()

    emst = ExtractMySQLTbl("weighted_strategy_set")
    emst.addhub("WeightedStrategySetID", src)
    emst.addlink("strategy_set", src, ["WeightedStrategySetID","StrategySetID"])
    emst.addsat(src, "WeightedStrategySetID", "WeightSummaryNum")
    emst.openconnections()
    partsel = "SELECT DISTINCT StrategySetID FROM weighted_strategy_set"
    emst.pullbypartition(partsel, "StrategySetID")
    emst.closeconnections()

    emst = ExtractMySQLTbl("weighted_strategy_set_member")
    emst.addhub("WeightedStrategySetMemberID", src)
    emst.addlink("weighted_strategy_set", src,
                 ["WeightedStrategySetID","WeightedStrategySetMemberID"])
    emst.addlink("strategy_set_member", src,
                 ["StrategySetMemberID","WeightedStrategySetMemberID"])
    emst.addsat(src, "WeightedStrategySetMemberID", "WeightNum")
    emst.openconnections()
    partsel = "SELECT DISTINCT StrategySetMemberID FROM weighted_strategy_set_member"
    emst.pullbypartition(partsel, "StrategySetMemberID")
    emst.closeconnections()

    emst = ExtractMySQLTbl("game_stats")
    emst.addhub("GameId", src)  # Mmmm... should be ID, not Id
    emst.addsat(src, "GameId", "RunTimeSecs,UpdateTs,FinishFlg")
    emst.openconnections()
    emst.pulldata()
    emst.closeconnections()

    emst = ExtractMySQLTbl("game_results")
    emst.addhub("GameId,PlayerPosNum", src)  # Mmmm... should be ID, not Id
    emst.addsat(src, "GameId,PlayerPosNum", "ScoreCnt,RankNum,WinFlg")
    emst.addlink("weighted_strategy_set", src,
                 ["GameId,PlayerPosNum", "WeightedStrategySetID"])
    emst.addlink("game_stats", src,
                 ["GameId,PlayerPosNum", "GameId"])
    emst.openconnections()
    emst.pulldata()
    emst.closeconnections()

    emst = ExtractMySQLTbl("agent_value")
    emst.addhub("WeightedStrategySetID", src)
    emst.addsat(src, "WeightedStrategySetID", "WinRate,ExecCnt,UpdateEpoch")
    emst.addlink("weighted_strategy_set", src,
                 ["WeightedStrategySetID","WeightedStrategySetID"])
    emst.openconnections()
    emst.pulldata()
    emst.closeconnections()

    emst = ExtractMySQLTbl("strategy")
    emst.addhub("StrategyID", src)
    emst.addsat(src, "StrategyID", "StrategyTxt")
    emst.openconnections()
    emst.pulldata()
    emst.closeconnections()

    emst = ExtractMySQLTbl("strategy_set")
    emst.addhub("StrategySetID", src)
    emst.addsat(src, "StrategySetID", "StrategySetTxt")
    emst.openconnections()
    emst.pulldata()
    emst.closeconnections()

    emst = ExtractMySQLTbl("competition_grp")
    emst.addhub("CompGrpId", src)
    emst.addsat(src, "CompGrpId", "CompGrpDescr,SetNum")
    emst.openconnections()
    emst.pulldata()
    emst.closeconnections()

    emst = ExtractMySQLTbl("competition_grp_member")
    emst.addhub("CompGrpId", src)
    emst.addlink("competition_grp", src,
                 ["CompGrpId","CompGrpId"])
    emst.addlink("weighted_strategy_set_member", src,
                 ["WeightedStrategySetMemberID","CompGrpId"])
    emst.openconnections()
    emst.pulldata()
    emst.closeconnections()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        src = sys.argv[1]
    else:
        src = "Win10.laptop.lrg1"
    processall(src)
