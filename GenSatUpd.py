import sys

def generate(flnm):
    stgdb = "dvaz1stg"
    tgtdb = "dvaz1"
    stgsch = "sch_ld1"
    tgtsch = "sch_tgt1"

    tblnm = None
    colz = []
    bizcolz = []
    hk = ""
    hd = ""
    fl = open(flnm)
    for ln in fl:
        # print(ln.strip())
        # Semi-colon indicates an execution
        if ";" in ln:
            if tblnm is None:
                continue
            # put together a load statement
            joins = "s.{0} = tgts.{0}".format(hk)
            qualcolz = ["s." + col for col in colz]
            # wher = ["h.{0} IS NULL".format(col)
            #         for col in bizcolz]
            # staging DB is the default, so do not include it in the FROM
            updstmt = """
            UPDATE {0}.{1}.{2}
            SET load_end_dt = mxld.next_ld_dt
            FROM
            (
            SELECT
                s1.{3},
                s1.load_dt,
                MIN(s2.load_dt) next_ld_dt
            FROM {0}.{1}.{2} s1
                JOIN {0}.{1}.{2} s2
                ON s1.{3} = s2.{3}
            WHERE s1.load_dt < s2.load_dt
                AND s1.load_end_dt IS NULL
            GROUP BY 1,2
            ) mxld
           WHERE {0}.{1}.{2}.{3} = mxld.{3}
                AND {0}.{1}.{2}.load_dt = mxld.load_dt
            """.format(tgtdb, tgtsch, tblnm, hk)
            print(updstmt)
            tblnm = None
            colz = []
            bizcolz = []
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
                if "hash_key" in flds[0].lower():
                    hk = flds[0]
    fl.close()

if __name__ == "__main__":
    flnm = sys.argv[1]
    generate(flnm)
