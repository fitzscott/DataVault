import mysql.connector
import dbcfg
import sys
import itertools as it

def setup():
    cnct = mysql.connector.connect(**dbcfg.dbcon)
    return (cnct)

def shutdown(cnct):
    cnct.close()

def chkdupes(narr, maxwgt=5):
    retval = False
    for chk in range(maxwgt - 1):
        cnum = chk + 2
        multcnt = 0
        for n in narr:
            if int(n) % cnum == 0:
                multcnt += 1
        if multcnt == len(narr):
            # print("Got multiple count " + str(multcnt) + " for " +
            #       "".join([str(x) for x in narr]))
            retval = True
            break
    # print("Checked " + nstr + ", returned " + str(retval))
    return (retval)

def fill1StratSet(cnct, ssid, siz=9):
    print("Filling strategy set " + str(ssid))
    curs = cnct.cursor(buffered=True)
    selwss = """
    SELECT WeightSummaryNum
    FROM weighted_strategy_set
    WHERE StrategySetID = {0}
    """.format(str(ssid))
    curs.execute(selwss)
    wsnlist = []
    for (wsn,) in curs:
        wsnlist.append(wsn)

    rng = [n + 1 for n in range(5)]
    allwsn = it.product(rng, repeat=siz)
    cnt = 0
    skipped = 0
    dups = 0
    for wsn in allwsn:
        # skip any non-"prime" weights, e.g. 242424
        if chkdupes(wsn):
            dups += 1
            continue
        # skip any already found
        wsnint = int("".join([str(w) for w in wsn]))
        if wsnint in wsnlist:
            skipped += 1
            continue
        wssinsstr = """
        INSERT INTO weighted_strategy_set (StrategySetID,
         WeightSummaryNum) VALUES ({0}, {1})
        """.format(ssid, wsnint)
        # print(wssinsstr)
        curs.execute(wssinsstr)
        wssid = curs.lastrowid
        winrt = 2.0
        execnt = 0
        epc = 0
        agvinsstr = """
        INSERT INTO agent_value (WeightedStrategySetID, WinRate,
         ExecCnt, UpdateEpoch) VALUES ({0}, {1}, {2}, {3})
        """.format(wssid, winrt, execnt, epc)
        # print(agvinsstr)
        curs.execute(agvinsstr)
        cnt += 1
        if cnt % 1000 == 0:
            cnct.commit()
            print("Completed " + str(cnt) + ", " + str(skipped) +
                  " skipped, " + str(dups) + " dupes")
    cnct.commit()   # finish off any remaining deletes
    print("Created " + str(cnt) + " rows, already had " + str(skipped) +
          ", " + str(dups) + "non-primes")
    print("Completed deletion of strategy set " + str(ssid))

if __name__ == "__main__":
    cnct = setup()
    fill1StratSet(cnct, int(sys.argv[1]))
    shutdown(cnct)
