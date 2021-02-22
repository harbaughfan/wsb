from commentdb import CommentDb
from symboldb import SymbolDb
from pushshift import PushShift

from collections import Counter
import time
import json
import re


class main:

    def __init__(self):
        print("Hello")

        # check epoch
        print("This needs to be 1/1/70: " + time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.gmtime(0)))

        # initialize some objects
        self.wsbdb = CommentDb("test.db")
        self.sym = SymbolDb("nasdaq.db")
        self.ps = PushShift()
        self.alphaonlyregex = re.compile("[^a-zA-Z[ ]")
        print("Ready")

        self.fetchcommentsfrompushshift()

        currenttime = int(time.time())
        endtime = currenttime
        starttime = currenttime-3600
        print(self.checkforstocks(starttime, endtime))
        print("The above is for comments between " + time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.gmtime(starttime)) + " and " + time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.gmtime(endtime)))

        print("Bye")

    def fetchcommentsfrompushshift(self):
        currenttime = int(time.time())
        mostrecententry = self.wsbdb.mostrecententry()
        print("Fetching Comments, Current Time: " + str(currenttime) + "   Most Recent Entry: " + str(mostrecententry) + " (" + str(mostrecententry-currenttime) + "sec)")

        # retrieves all comments from pushshift between most recent entry in the database & the current time
        psrawresult = self.ps.retrieve(mostrecententry, currenttime)

        # TODO: if capped at 100 rows then iterate for more

        # convert the raw comments into a json object
        psjsonresult = json.loads(psrawresult)

        # load the json object into the database
        self.wsbdb.loadjson(psjsonresult["data"])

        currenttime = int(time.time())
        mostrecententry = self.wsbdb.mostrecententry()
        print("Loaded new rows, Current Time: " + str(currenttime) + "   Most Recent Entry: " + str(mostrecententry) + " (" + str(mostrecententry-currenttime) + "sec)")


    def findsymbolsincomment(self, cmt):
        # Clean up the comment to leave alpha & spaces only
        cleaned = self.alphaonlyregex.sub('', cmt)

        stocklist = []

        # for each word in the comment
        for token in cleaned.split():
            # if the word is a symbol save it
            if self.sym.isasymbol(token):
                stocklist.append(token)

        return stocklist

    def checkforstocks(self, starttime=0, endtime=int(time.time())):
        cmts = self.wsbdb.getcommentsbetween(starttime, endtime)
        stocklist = []
        #for every comment between the start and end times
        for cmt in cmts:

            # for every stock symbol within the comment
            for sym in self.findsymbolsincomment(str(cmt[0])):
                # add the stock symbol to the list
                stocklist.append(sym)

        # return the number of times each symbol appears in the list
        return Counter(stocklist)
