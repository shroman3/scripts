'''
Created on Sep 15, 2017

@author: shroman
'''

import sqlite3, sys, pandas

def todb(filename):
    tablename = filename.split('.')[0]
    with open(filename, 'r') as inp:
        columns = inp.readline()
        
    con = sqlite3.connect("%s.db" % tablename)
#     cur = con.cursor()
#     q = "CREATE TABLE %s (%s)" % (tablename, columns) 
#     cur.execute(q)
    
    with open(filename, 'r') as inp:
        df = pandas.read_csv(inp)
        df = df.pipe(lambda d: d[d['exp'] != 'exp'])
        df.to_sql(tablename, con, if_exists='append', index=False)


if __name__ == "__main__":
    if (len(sys.argv) != 2):
        print('Please call the todb in the following manner:')
        print("todb.py filename")
        exit(0)
    todb(sys.argv[1])