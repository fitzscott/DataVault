import sys

def generate(flnm):
    stgdb = "dvaz1stg"
    tgtdb = "dvaz1"
    stgsch = "sch_ld1"
    tgtsch = "sch_tgt1"

    tblnm = None
    colz = []
    frgnkeycolz = []
    fl = open(flnm)
    for ln in fl:
        # print(ln.strip())
        # Semi-colon indicates an execution
        if ";" in ln:
            if tblnm is None:
                continue
            # put together a load statement
            joins = ["l.{0} = tgtl.{0}".format(col)
                     for col in frgnkeycolz]
            qualcolz = ["l." + col for col in colz]
            wher = ["tgtl.{0} IS NULL".format(col)
                    for col in frgnkeycolz]
            # staging DB is the default, so do not include it in the FROM
            inssel = """
            INSERT INTO {0}.{3}.{1}
            (
                {2},
                load_dt
            )
            SELECT 
                {7},
                CURRENT_TIMESTAMP()
            FROM {4}.{5}.{1} l
                LEFT OUTER JOIN {0}.{3}.{1} tgtl
                ON {6}
            WHERE
                {8}
            """.format(tgtdb, tblnm, ",\n\t\t\t\t".join(colz), tgtsch, stgdb,
                       stgsch, "\n\t\t\t\tAND ".join(joins),
                       ",\n\t\t\t\t".join(qualcolz),
                       "\n\t\t\t\tAND ".join(wher))
            print(inssel)
            tblnm = None
            colz = []
            frgnkeycolz = []
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
                if "hash_key" in flds[0].lower() and \
                        "_".join(tblnm.split("_")[0:-2]) not in \
                        flds[0].lower():
                    # print("Adding " + flds[0] + " to FKs for " + tblnm)
                    frgnkeycolz.append(flds[0])
    fl.close()

if __name__ == "__main__":
    flnm = sys.argv[1]
    generate(flnm)
