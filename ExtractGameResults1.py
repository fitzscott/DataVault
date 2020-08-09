import sys
import time
from datetime import datetime
import hashlib as hl
import subprocess
# import os
import ApplyResults1 as ar


class ExtractGameResults1():
    """
    Pull game results data out of the files recording manual games played
    against the standard computer strategy sets.
    There will be files produced for hubs, links, & satellites related to
    game results (not stats, though), strategy, strategy set, strategy set
    member, weighted strategy set.
    """
    def __init__(self, src):
        self._flnm = None
        self._src = src
        self._files = {}
        # These will be the prefixes for the produced files.
        self._bsflnms = ["gr", "wssLgr", "wss", "ssLwss", "ss", "ssLssm",
                         "ssm", "sLssm", "s"]
        self._epc = str(time.mktime(datetime.today().timetuple())).split(".")[0]
        self._delim = "|"
        self._ranks = []
        self._numplyrz = 4
        self._flnms = {}

    @property
    def files(self):
        return (self._files)

    @property
    def filenames(self):
        return (self._flnms)

    @property
    def delim(self):
        return (self._delim)

    @property
    def src(self):
        return (self._src)

    @property
    def epc(self):
        return (self._epc)

    def startup(self):
        # Create the files needed to represent the vault style for the
        # game results data.
        for bf in self._bsflnms:
            # if it has an "L" in it, it's a link
            if "L" in bf:
                flnm = bf + "_L_" + self._epc + ".txt"
                fl = open(flnm, "w")
                self._files[bf] = fl
                self._flnms[bf] = flnm
            # otherwise, it's a hub + a satellite
            else:
                for ty in ["H", "S"]:
                    if ty == "S" and bf != "gr":    # don't need many satellites
                        continue
                    flnm = "_".join([bf, ty, self._epc]) + ".txt"
                    fl = open(flnm, "w")
                    self._files[bf + ty] = fl
                    self._flnms[bf + ty] = flnm

    def shutdown(self):
        for flkey in self._files.keys():
            self._files[flkey].close()

    def mkhash(self, strarr, inclrec=True):
        tohash = [s for s in strarr]
        if inclrec:
            tohash.append(self._src)
        hval = hl.md5(self._delim.join(tohash).encode("utf-8")).hexdigest()
        return (hval)

    def writerec(self, flkey, recarr):
        self.files[flkey].write(self.delim.join(recarr) + "\n")
        # print(self.delim.join(recarr))

    def processrecord(self, rec, recnum, rank):
        """
        Game records look like this:
        01131454 Loser:  WeightedComboStrategyPlayer: ExactFitStrategy TopRowsStrategy . . . final score = 64
        i.e.:
            Game ID
            Winner: or Loser:
            Strategy combination class: Ignore for now
            A sequence of strategies in the strategy set used by this player
            "final score = "
            Its final score
        :param rec: A record in the format above
        :param recnum: Determines player position number
        :return: None
        Write individual records to specific files
        """
        gameid = rec[0]
        winloss = rec[1][0]
        strats = rec[3:-4]
        score = rec[-1]
        posnum = str(recnum)
        if winloss == "W":
            rank = 1        # can have multiple winners, all rank 1
        srank = str(rank)
        stratset = "+".join(strats)
        # print("_".join([gameid, winloss, stratset, score, posnum, srank]))
        src = self._src
        # write game results hub
        grhk = self.mkhash([gameid, posnum])
        self.writerec("grH", [src, grhk, gameid, posnum])
        # write game results satellite
        grhd = self.mkhash(([score, srank, winloss]))
        self.writerec("grS", [src, grhk, grhd, score, srank, winloss])
        # write strategy set hub
        sshk = self.mkhash([stratset])
        self.writerec("ssH", [src, sshk, stratset])
        # do not need a strategy set satellite for this data
        # write weighted strategy set hub
        wgtsum = 0
        mult = 1
        slen = len(strats)
        for idx in range(slen):
            wgtsum += max(1, idx-slen+6) * mult
            mult *= 10
        # print("weight summary = " + str(wgtsum))
        wsshk = self.mkhash([stratset, str(wgtsum)])
        # Business keys strategy set + weight
        self.writerec("wssH", [src, wsshk, stratset, str(wgtsum)])
        # do not need a weighted strategy set satellite, either
        # write weighted strategy set <-> game results link
        wssLgrhk = self.mkhash([stratset, str(wgtsum), gameid, posnum])
        self.writerec("wssLgr", [src, wssLgrhk, wsshk, grhk])
        # write strategy set <-> weighted strategy set link
        ssLwsshk = self.mkhash([stratset, str(wgtsum)])     # need 2x?
        self.writerec("ssLwss", [src, ssLwsshk, sshk, wsshk])
        # write strategy set member & strategy
        for strat in strats:
            # write strategy set member hub
            ssmhk = self.mkhash([strat, stratset])
            # Now thinking that the strategy set member hub is designed wrong:
            # The member ID is really the strategy + strategy set,
            # so a link would do. Except that the problem with the weighted
            # strategy set member comes up again.
            # The biz keys are strategy + strategy set
            self.writerec("ssmH", [src, ssmhk, strat, stratset])
            # write strategy set <-> strategy set member link
            ssLssmhk = self.mkhash([stratset, strat, stratset])
            self.writerec("ssLssm", [src, ssLssmhk, sshk, ssmhk])
            # write strategy hub - no satellite needed
            shk = self.mkhash([strat])
            self.writerec("sH", [src, shk, strat])
            # write strategy <-> strategy set member link
            sLssmhk = self.mkhash([strat, strat, stratset])
            self.writerec("sLssm", [src, sLssmhk, shk, ssmhk])


    def processgame(self, gameset, scores):
        # self.processrecord(ln.strip(), recnum)
        ranks = [(idx, scores[idx]) for idx in range(len(scores))]
        #ranks.sort(key=getscore, reverse=True)    # higher is better
        ranks.sort(key=lambda sc: sc[1], reverse=True)  # higher is better
        # print(str(ranks))
        ranked = {}
        for rnkidx in range(len(ranks)):
            ranked[ranks[rnkidx][0]] = rnkidx + 1
        # print(str(ranked))
        for recidx in range(len(gameset)):
            self.processrecord(gameset[recidx], recidx, ranked[recidx])

    def processfile(self, flnm):
        self._flnm = flnm   # probably not needed
        self.startup()

        infl = open(flnm)
        recnum = 0
        gameset = []
        scores = []
        # Need to process the game as one set to get the ranks of the
        # individual players.
        for ln in infl:
            flds = ln.strip().split()
            assert (len(flds) > 5)
            gameset.append(flds)
            scores.append(int(flds[-1]))
            if len(gameset) == self._numplyrz:
                self.processgame(gameset, scores)
                gameset = []
                scores = []
            recnum += 1

        self.shutdown()
        # Now we have all the output files, potentially with duplicates.
        # Run a simple script to clean them up.
        subprocess.run(["powershell", r".\mkuniq.ps1", self._epc])
        # os.system(r"powershell .\mkuniq.ps1 " + self._epc)

        # now send the files to Snowflake & apply them against final tables.
        apgmrs = ar.ApplyResults1()
        for stgtblnm in self.files.keys():
            apgmrs.processfile(self.filenames[stgtblnm], stgtblnm)
            subprocess.run(["powershell", "Remove-Item", ".\\" + self.filenames[stgtblnm]])

if __name__  == "__main__":
    if len(sys.argv) < 2:
        print("usage: " + sys.argv[0] + "gameResultsFile [source]")
        sys.exit(-1)
    flnm = sys.argv[1]
    if len(sys.argv) > 2:
        src = sys.argv[2]
    else:
        src = "Win10.laptop.sm2"

    egr = ExtractGameResults1(src)
    egr.processfile(flnm)
