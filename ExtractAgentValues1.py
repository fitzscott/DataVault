import sys
import subprocess
import ExtractGameResults1 as egr
import ApplyResults1 as ar


class ExtractAgentValues1(egr.ExtractGameResults1):
    """
    Extract data from agent values files, load it into files suitable for
    data vault tables, then load those DV tables.
    """
    def __init__(self, src):
        super().__init__(src)

    def startup(self):
        # smaller than the game results set
        for bsflnm in ["avH", "avS", "wssLav", "wssH", "ssLwss", "ssH",
                       "sH", "ssmH", "sLssm", "ssLssm"]:
            flnm = "_".join([bsflnm, self.epc]) + ".txt"
            fl = open(flnm, "w")
            self._files[bsflnm] = fl
            self._flnms[bsflnm] = flnm

    def processstrats(self, stratset):
        src = self.src
        # write strategy set hub
        sshk = self.mkhash([stratset])
        self.writerec("ssH", [src, sshk, stratset])
        strats = stratset.split("+")
        for strat in strats:
            # write strategy hub
            shk = self.mkhash([strat])
            self.writerec("sH", [src, shk, strat])
            # write strategy set member hub
            ssmhk = self.mkhash([strat, stratset])
            # The biz keys are strategy + strategy set
            self.writerec("ssmH", [src, ssmhk, strat, stratset])
            # write strategy <-> strategy set member link
            sLssmhk = self.mkhash([strat, strat, stratset])
            self.writerec("sLssm", [src, sLssmhk, shk, ssmhk])
            # write strategy set <-> strategy set member link
            ssLssmhk = self.mkhash([stratset, strat, stratset])
            self.writerec("ssLssm", [src, ssLssmhk, sshk, ssmhk])
        return (sshk)

    def processvalues(self, stratset, wgt, winrt, cnt, sshk):
        src = self.src
        # write weighted strategy set hub
        wsshk = self.mkhash([stratset, wgt])
        # Business keys strategy set + weight
        self.writerec("wssH", [src, wsshk, stratset, wgt])
        # write strategy set <-> weighted strategy set link
        ssLwsshk = self.mkhash([stratset, wgt])     # need 2x?
        self.writerec("ssLwss", [src, ssLwsshk, sshk, wsshk])
        # write agent value hub
        avhk = self.mkhash([stratset, wgt])    # same as WSS
        self.writerec("avH", [src, avhk, stratset, wgt])
        # write agent value satellite
        avhd = self.mkhash([winrt, cnt])    # do not want epoch in the hashdiff
        self.writerec("avS", [src, avhk, avhd, winrt, cnt, self.epc])
        # write weighted strategy set <-> agent values link
        wssLavhk = self.mkhash([stratset, wgt, stratset, wgt]) # necessary?
        self.writerec("wssLav", [src, wssLavhk, wsshk, avhk])

    def processfile(self, flnm):
        self._flnm = flnm   # probably not needed
        self.startup()
        infl = open(flnm)
        # Guaranteed only one line in the file.
        for ln in infl:
            stratset, valset = ln.strip().split("|")
            sshk = self.processstrats(stratset)
            valz = valset.split(",")
            for val in valz:
                vparts = val.split(":")
                wgt = vparts[0]
                winrt = vparts[1]
                if len(vparts) == 2:
                    cnt = "1"
                else:
                    cnt = vparts[2]
                self.processvalues(stratset, wgt, winrt, cnt, sshk)
            break           # Skip any others.
        self.shutdown()
        # now send the files to Snowflake & apply them against final tables.
        apgmrs = ar.ApplyResults1()
        for stgtblnm in self.files.keys():
            apgmrs.processfile(self.filenames[stgtblnm], stgtblnm)
            subprocess.run(["powershell", "Remove-Item", ".\\" + self.filenames[stgtblnm]])

if __name__  == "__main__":
    if len(sys.argv) < 2:
        print("usage: " + sys.argv[0] + "agentValuesFile [source]")
        sys.exit(-1)
    flnm = sys.argv[1]
    if len(sys.argv) > 2:
        src = sys.argv[2]
    else:
        src = "Ubuntu.laptop.Big1"

    eav = ExtractAgentValues1(src)
    eav.processfile(flnm)
