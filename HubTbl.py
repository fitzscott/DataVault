import DepTbl as dt
import hashlib as hl


class HubTbl(dt.DepTbl):
    """
    HubTbl - output for hub table in data vault
    A hub is comprised of keys.
    The main key is a hashed combination of the business keys.
    The business keys are stored individually.
    """

    def __init__(self, tnm, src="SYSTEM"):
        super().__init__(tnm, "H", src)

    def output(self, colnms, row):
        outvalz = {0: self.src}

        hval = self.mkhashedkey(colnms, row)
        outvalz[1] = hval
        for cidx in range(len(colnms)):
            if colnms[cidx] in self.keycolumns:
                # print("Found " + colnms[cidx])
                kidx = self.keycolumns.index(colnms[cidx])
                val = str(row[cidx])
                outvalz[kidx + 2] = val
        # print("hub out values " + str(outvalz))
        towrt = self.delimiter.join([str(outvalz[idx])
                                     for idx in sorted(outvalz.keys())])
        self.outputfile.write(towrt + "\n")

