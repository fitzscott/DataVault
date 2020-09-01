import mysql.connector
import dbcfg
import sys

def setup():
    cnct = mysql.connector.connect(**dbcfg.dbcon)
    return (cnct)

def shutdown(cnct):
    cnct.close()

def clear1WSS(cnct, wssid):
    tbls = ["game_results", "agent_value",
            "weighted_strategy_set_member", "weighted_strategy_set"]
    # do we also need "competition_grp_member",?
    curs = cnct.cursor()
    for tbl in tbls:
        delstmt = """
        DELETE FROM {0}
        WHERE WeightedStrategySetID = {1}
        """.format(tbl, str(wssid))
        # print(delstmt)
        curs.execute(delstmt)
        # cnct.commit()       # not sure if we need this

def clear1StratSet(cnct, ssid):
    print("Deleting references to strategy set " + str(ssid))
    curs = cnct.cursor(buffered=True)
    selwss = """
    SELECT WeightedStrategySetID 
    FROM weighted_strategy_set
    WHERE StrategySetID = {0}
    """.format(str(ssid))
    curs.execute(selwss)
    cnt = 0
    for (wss,) in curs:
        clear1WSS(cnct, wss)
        cnt += 1
        if cnt % 1000 == 0:
            cnct.commit()
            print("Completed " + str(cnt))
    cnct.commit()   # finish off any remaining deletes
    print("Completed deletion of strategy set " + str(ssid))

if __name__ == "__main__":
    cnct = setup()
    clear1StratSet(cnct, int(sys.argv[1]))
    shutdown(cnct)
