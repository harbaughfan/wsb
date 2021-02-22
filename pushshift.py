import json
import urllib.request
import time

class PushShift:

    def __init__(self):
        self.timeoflastretrieve = 0
        self.throttleseconds = 20

    def retrieve(self, starttime, endtime):
        if (self.throttleok()):
            urlstring = "https://api.pushshift.io/reddit/search/comment/?subreddit=wallstreetbets" \
            "&sort=desc" \
            "&sort_type=created_utc" \
            "&after=" + str(starttime) + \
            "&before=" + str(endtime) + \
            "&size=1000"
            print("Opening: " + urlstring)
            self.timeoflastretrieve = int(time.time())
            return urllib.request.urlopen(urllib.request.Request(urlstring)).read()

    def throttleok(self):
        return (self.timeoflastretrieve + self.throttleseconds < int(time.time()))