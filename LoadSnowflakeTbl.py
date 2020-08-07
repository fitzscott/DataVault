import snowflake.connector
import dbcfg
import snoflkcon
import os
import sys

class LoadSnowflakeTbl():
    """

    """
    def __init__(self, con=None):
        self._cnct = con

    def connect(self):
        if self._cnct is None:
            self._cnct = snoflkcon.get_connection()
            assert(self._cnct is not None)

    def disconnect(self):
        if self._cnct is not None:
            self._cnct.close()

    def loadfromfile(self, flnm):
        basetblnm = "_".join(os.path.basename(flnm).split("_")[0:-1])
        lobnm = basetblnm.lower()
        if lobnm in snoflkcon.flnm2tbl.keys():
            tblnm = snoflkcon.flnm2tbl[basetblnm.lower()]
        else:
            tblnm = lobnm
        print("Loading " + flnm + " into table " + tblnm)
        curs = self._cnct.cursor()
        putcmd = r"""
        PUT file://{0} @%{1}
        """.format(flnm, tblnm)
        print(putcmd)
        curs.execute(putcmd)
        cpcmd = r"""
        COPY INTO {0}
        FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|')
        """.format(tblnm)
        print(cpcmd)
        curs.execute(cpcmd)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: " + sys.argv[0] + " loadFile")
    # print("Snowflaky")
    lsft = LoadSnowflakeTbl()
    lsft.connect()
    # lsft.loadfromfile(r"E:\data\Snowflake\weighted_strategy_set_member_H_1595805595.txt")
    # lsft.loadfromfile(r"E:\data\Snowflake\strategy_H_1595856919.txt")
    # lsft.loadfromfile(r"E:\data\Snowflake\strategy_S_1595856919.txt")
    # lsft.loadfromfile(r"E:\data\Snowflake\strategy_set_member_H_1595800776.txt")
    # lsft.loadfromfile(r"E:\data\Snowflake\strategy_set_member_weighted_strategy_set_member_L_1595802139.txt")
    lsft.loadfromfile(sys.argv[1])
    lsft.disconnect()

