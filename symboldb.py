import sqlite3

class SymbolDb:

    def __init__(self, filespec, table="tickers"):
        self.table = table

        # Connect to database file
        self.conn = sqlite3.connect(filespec)
        print("Opened " + filespec + " with " + str(self.numberofrows()) + " rows.")

        # allows us to look for rows by name instead of number
        self.conn.row_factory = sqlite3.Row

        # get the entire table
        self.dbresult = self.conn.execute("select * from " + self.table)

        # load all the ticker symbols into a python list
        self.tickers = [row["symbol"] for row in self.dbresult]
        print("Loaded " + str(len(self.tickers)) + " symbols.")

    def numberofrows(self):
        return self.conn.execute("select count(*) from "+self.table).fetchone()[0]

    def isasymbol(self,str):
        return str in self.tickers
