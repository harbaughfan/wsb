import sqlite3
import json


class CommentDb:

    def __init__(self, filespec, table="wsb"):
        self.table = table
        # Connect to database file, creating the file if it doesn't exist
        self.conn = sqlite3.connect(filespec)

        # allows us to look for rows by name instead of number
        self.conn.row_factory = sqlite3.Row

        self.cols = [
            "all_awardings",
            "associated_award",
            "author",
            "author_flair_background_color",
            "author_flair_css_class",
            "author_flair_template_id",
            "author_flair_text",
            "author_flair_text_color",
            "awarders",
            "body",
            "collapsed_because_crowd_control",
            "comment_type",
            "created_utc",
            "gildings",
            "id",
            "is_submitter",
            "link_id",
            "locked",
            "no_follow",
            "parent_id",
            "permalink",
            "retrieved_on",
            "score",
            "send_replies",
            "stickied",
            "subreddit",
            "subreddit_id",
            "top_awarded_type",
            "total_awards_received",
            "treatment_tags",
            "author_flair_richtext",
            "author_flair_type",
            "author_fullname",
            "author_patreon_flair",
            "author_premium"]

        # Creates the wsb table if it doesn't already exist
        createstr = "create table if not exists " + self.table + " " \
                    "(" \
                    "all_awardings text, " \
                    "associated_award text, " \
                    "author text, " \
                    "author_flair_background_color text, " \
                    "author_flair_css_class text, " \
                    "author_flair_template_id text, " \
                    "author_flair_text text, " \
                    "author_flair_text_color text, " \
                    "awarders text, " \
                    "body text, " \
                    "collapsed_because_crowd_control text, " \
                    "comment_type text,created_utc text, " \
                    "gildings text, " \
                    "id text, " \
                    "is_submitter text, " \
                    "link_id text, " \
                    "locked text, " \
                    "no_follow text, " \
                    "parent_id text, " \
                    "permalink text, " \
                    "retrieved_on text, " \
                    "score text, " \
                    "send_replies text, " \
                    "stickied text, " \
                    "subreddit text, " \
                    "subreddit_id text, " \
                    "top_awarded_type text, " \
                    "total_awards_received text, " \
                    "treatment_tags text, " \
                    "author_flair_richtext text, " \
                    "author_flair_type text, " \
                    "author_fullname text, " \
                    "author_patreon_flair text, " \
                    "author_premium text " \
                    ")"
        self.conn.execute(createstr)
        print("Opened " + filespec + " with " + str(self.numberofrows()) + " rows.")

    def numberofrows(self):
        return self.conn.execute("select count(*) from " + self.table).fetchone()[0]

    def mostrecententry(self):
        return int(self.conn.execute("select coalesce(max(created_utc),0) from " + self.table).fetchone()[0])

    def loadjson(self, rawjson):
        # Get data lined up into columns
        value = []
        values = []
        for data in rawjson:
            for i in self.cols:
                value.append(str(dict(data).get(i)))
            values.append(list(value))
            value.clear()
        insertstr = "insert into " + self.table + " ({0}) values (?{1})".format(",".join(self.cols), ",?" * (len(self.cols) - 1))
        self.conn.executemany(insertstr, values)
        self.conn.commit()
        print("Database now contains " + str(self.numberofrows()) + " rows.")

    def getcommentsbetween(self, starttime, endtime):
        return self.conn.execute("select body from " + self.table + " where created_utc > " + str(starttime) + " and created_utc < " + str(endtime))


