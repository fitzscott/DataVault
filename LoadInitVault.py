import snoflkcon
import sys

def ldTgtFromStg(flnm):
    # Read through a DDL script, pulling table & column names from the
    # CREATE TABLE statements. Skip load date columns, as they will be
    # populated automatically.
    # This is only useful as an initial / historical load.  There is no
    # change detection or the like.

    cnct = snoflkcon.get_connection()
    curs = cnct.cursor()

    stgdb = "dvaz1stg"
    tgtdb = "dvaz1"
    stgsch = "sch_ld1"
    tgtsch = "sch_tgt1"

    tblnm = None
    colz = []
    fl = open(flnm)
    for ln in fl:
        # print(ln.strip())
        # Semi-colon indicates an execution
        if ";" in ln:
            if tblnm is None:
                continue
            # put together a load statement
            # staging DB is the default, so do not include it in the FROM
            inssel = """
            INSERT INTO {0}.{3}.{1}
            ({2})
            SELECT {2}
            FROM {4}.{5}.{1}
            """.format(tgtdb, tblnm, ",".join(colz), tgtsch, stgdb, stgsch)
            print(inssel)
            curs.execute(inssel)
            tblnm = None
            colz = []
            continue
        flds = ln.lstrip().strip().split()
        if len(flds) < 1:
            continue
        if flds[0].upper() == "CREATE":
            tblnm = flds[2]
        elif len(flds) > 1:
            skipchk = flds[0].upper()
            # skip the primary index statement & the load-related columns
            if skipchk != "PRIMARY" and skipchk[0:4] != "LOAD" and\
                    skipchk != "DROP":
                colz.append(flds[0])
    fl.close()
    cnct.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: " + sys.argv[0] + " scriptDDLfile")
        sys.exit(-1)
    ldTgtFromStg(sys.argv[1])
