import sys

def generate(flnm):
    stgdb = "dvaz1stg"
    tgtdb = "dvaz1"
    stgsch = "sch_ld1"
    tgtsch = "sch_tgt1"

    tblnm = None
    colz = []
    fl = open(flnm)
    for ln in fl:
        # print(ln.strip())
        if "DROP" in ln:
            print(ln.strip())
            continue
        # Semi-colon indicates an execution
        if ";" in ln:
            if tblnm is None:
                continue
            # staging DB is the default, so do not include it in the FROM
            mktbl = """
            CREATE TABLE {0}.{3}.{1}
            (
                record_src VARCHAR(40),
                {2}
            );
            """.format(stgdb, tblnm, ",\n\t\t\t\t".join(colz), stgsch)
            print(mktbl)
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
            if skipchk[0:4] != "LOAD" and skipchk != "DROP" \
                    and skipchk != "RECORD_SRC":
                if skipchk == "PRIMARY":
                    colz.append(" ".join(flds))
                else:
                    colz.append(" ".join(flds)[0:-1])
    fl.close()

if __name__ == "__main__":
    flnm = sys.argv[1]
    generate(flnm)
