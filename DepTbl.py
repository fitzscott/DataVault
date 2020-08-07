import sys
import os
import time
from datetime import datetime
import hashlib as hl
import dbcfg

class DepTbl():
    """
    Translate normalized structure into data vault structure
    """
    def __init__(self, tnm, ext, src="SYSTEM"):
        self._delim = "|"
        self._tblnm = tnm
        self._src = src
        self._fl = None
        self._keycolz = []
        self._nonkeycolz = []
        self._epc = None
        self._savdir = dbcfg.stagedir
        self._ext = ext

    @property
    def name(self):
        return (self._tblnm)

    @property
    def keycolumns(self):
        return (self._keycolz)

    @property
    def nonkeycolumns(self):
        return (self._nonkeycolz)

    @property
    def delimiter(self):
        return (self._delim)

    @property
    def src(self):
        return (self._src)

    @property
    def outputdir(self):
        return (self._savdir)

    @outputdir.setter
    def outputdir(self, val):
        self._savdir = val

    @property
    def outputfile(self):
        return (self._fl)

    @property
    def ext(self):
        return (self._ext)

    def filename(self):
        if self._epc is None:
            self._epc = str(time.mktime(datetime.today().timetuple())).split(".")[0]
        self._flnm = "_".join([self._tblnm, self._ext, self._epc]) + ".txt"
        return (self._savdir + self._flnm)

    def startup(self):
        self._fl = open(self.filename(), "w")

    def clearepoch(self):
        self._epc = None

    def shutdown(self):
        self._fl.close()
        self.clearepoch()

    def __str__(self):
        retval = "Table " + self.name + "\n"
        retval += " " * 4 + "Key columns " + str(self.keycolumns) + "\n"
        if len(self.nonkeycolumns) > 0:
            retval += " " * 4 + "Non-key columns " + str(self.nonkeycolumns)
        else:
            retval += " " * 4 + "No non-key columns"
        return (retval)

    def addcolz(self, colzstr, tgt):
        """
        Add individual column names from a comma-separated string.
        :param colzstr: comma-delimited string of column names
        :return:
        """
        for col in colzstr.strip().split(","):
            tgt.append(col)      # .lower())

    def addkeycolz(self, colzstr):
        self.addcolz(colzstr, self._keycolz)

    def addnonkeycolz(self, colzstr):
        self.addcolz(colzstr, self._nonkeycolz)

    def mkhashedkey(self, colnms, ro, keycolz=None):
        """
        Given a set of column names, a row of results, and the key
        columns member, produce the hashed key.
        :param colnms: list of column names
        :param ro: row result from select
        :param keycolz: key columns (use member if None)
        :return: hashed key
        """
        key2enc = []
        if keycolz is None:
            keycolz = self.keycolumns
        for cidx in range(len(colnms)):
            if colnms[cidx] in keycolz:
                # kidx = keycolz(colnms[cidx])
                val = str(ro[cidx])
                key2enc.append(val)
        key2enc.append(self.src)
        # hash the delimited string of business keys
        hval = hl.md5(self.delimiter.join(key2enc).encode("utf-8")).hexdigest()
        return (hval)

    def output(self, colnms, row):
        pass
