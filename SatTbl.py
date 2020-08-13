import time
from datetime import datetime
import hashlib as hl
import DepTbl as dt


class SatTbl(dt.DepTbl):
    """
    SatTbl - output appropriate
    """
    def __init__(self, tnm, src="SYSTEM"):
        super().__init__(tnm, "S", src)
        self._lepc = None
        self._parnm = None

    @property
    def parenttable(self):
        return (self._parnm)

    @parenttable.setter
    def parenttable(self, val):
        self._parnm = val

    def output(self, colnms, row):
        # Satellite output will be:
        #    Record source
        #    Hash key
        #    Hash diff
        #    Descriptive columns
        outvalz = {0: self.src}
        hval = self.mkhashedkey(colnms, row)
        outvalz[1] = hval
        val2hash4dif = {}
        testprn = []

        for cidx in range(len(colnms)):
            if colnms[cidx] in self.nonkeycolumns:
                kidx = self.nonkeycolumns.index(colnms[cidx])
                val = str(row[cidx])
                # testprn.append(val)
                val2hash4dif[kidx] = val
                outvalz[kidx + 3] = val
        # print("^".join(testprn))p
        dif2h = self.delimiter.join([str(val2hash4dif[idx])
                                     for idx in sorted(val2hash4dif.keys())])
        # print("to hash: " + str(dif2h))
        outvalz[2] = hl.md5(dif2h.encode("utf-8")).hexdigest()
        # print("Out values: " + str(outvalz))
        towrt = self.delimiter.join([str(outvalz[idx])
                                     for idx in sorted(outvalz.keys())])
        # print("to write: " + str(towrt))
        self.outputfile.write(towrt + "\n")


