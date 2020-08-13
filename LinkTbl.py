import time
from datetime import datetime
import DepTbl as dt


class LinkTbl(dt.DepTbl):
    """
    LinkTbl - output appropriate
    """
    def __init__(self, tnm, src="SYSTEM"):
        super().__init__(tnm, "L", src)
        self._lepc = None
        self._parnm = None
        self._linkkeycols = []      # links have multiple sets of key columns

    @property
    def parenttable(self):
        return (self._parnm)

    @parenttable.setter
    def parenttable(self, val):
        self._parnm = val

    @property
    def linkkeycols(self):
        return (self._linkkeycols)

    def addkeycolz(self, colzarr):
        # overriding this with a different type for the colummns
        for colz in colzarr:    # end up with a list of lists
            c = []
            super().addcolz(colz, c)
            self._linkkeycols.append(c)

    def output(self, colnms, row):
        # Link output will be:
        #    Record source
        #    Link hash key
        #    Foreign hash key 1
        #    Foreign hash key 2
        #    ...
        #    Descriptive columns (if any)
        # outvalz = {0: self.src}
        outvalz = {0: self.src}
        concatkey = {}
        # testprn = []

        ohk = []
        idx = 2
        # make the individual hash keys
        for lkc in self.linkkeycols:
            ohk += lkc
            hval = self.mkhashedkey(colnms, row, lkc)
            outvalz[idx] = hval
            idx += 1
        # make the overall link hash key
        hval = self.mkhashedkey(colnms, row, ohk)
        outvalz[1] = hval
        # Not handling descriptive columns - perhaps in the future
        towrt = self.delimiter.join([str(outvalz[idx])
                                     for idx in sorted(outvalz.keys())])
        self.outputfile.write(towrt + "\n")

    def clearepoch(self):
        super().clearepoch()    # shouldn't matter
        self._lepc = None

    def filename(self):
        if self._lepc is None:
            self._lepc = str(time.mktime(datetime.today().timetuple())).split(".")[0]
        self._flnm = "_".join([self.name, self.parenttable, self.ext, self._lepc]) + ".txt"
        return (self.outputdir + self._flnm)

    def tablename(self):
        return ("_".join([self.name, "x", self.parenttable, self.ext]))
