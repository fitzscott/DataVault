# brain-dead splitter for Windows

import sys
import os

def splitintoXpieces(flnm, numpieces):
    bsnm = os.path.basename(flnm)
    dirnm = os.path.dirname(flnm)
    bsbs, bsext = bsnm.split(".")
    outflnms = [dirnm + "\\" + bsbs + "n" + str(n) + "." + bsext
                for n in range(numpieces)]
    outfls = []
    # print(str(outflnms))
    for outflnm in outflnms:
        outfl = open(outflnm, "w")
        outfls.append(outfl)
    infl = open(flnm)
    recnum = 0
    for ln in infl:
        # round robin
        outfls[recnum % numpieces].write(ln)    # don't bother stripping
        recnum += 1
    infl.close()
    for outfl in outfls:
        outfl.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: " + sys.argv[1] + "file2split #splits")
        sys.exit(-1)

    flnm = sys.argv[1]
    pieces = int(sys.argv[2])
    splitintoXpieces(flnm, pieces)
