"""
Merge listing tables within the SQL database across days into data set groups.
Duplicate listings are removed and a new variable is added the date on which the
listing was most recently scraped.  
"""
import pandas as pd
import sqlite3
import numpy as np

def main(db_name, table_list, table_name):
  """
  Parameters
  ----------
    db_name: str
      - name of the database in which the tables are saved.
    table_list: list of str
      - list of table names that should be merged into the database
    table_name: str
      - name of the new merged table
  """


    #establish a connection to a sql database, if it does not already exist, it is created
    #note that rentnyc is the name of the database and it can have multiple internal tables
    print "Connecting to %s database." % (db_name)
    con = sqlite3.connect(db_name)
    c = con.cursor()
    print "Done.\n"
    print "Begin formatting data."

    #read the first table in the list
    df = pd.read_sql("SELECT * FROM %s;" % (table_list[0]), con)

    #loop over remainder of list, merge the dataframes, drop duplicates data_id, but
    #keep the newer one
    for t in np.arange(1,np.shape(table_list)[0]):
        #print
        print "Now reading %s (%d/%d)" % (table_list[t],t,np.shape(table_list)[0]-1)
        #read in this table
        df_tp = pd.read_sql("SELECT * FROM %s;" % (table_list[t]), con)
        #merge and drop older duplicates
        df = pd.concat([df,df_tp]).drop_duplicates('data_id','last')

    #if the merged table already exists, drop it
    c.execute("""
    DROP TABLE IF EXISTS %s;
    """ % (table_name))

    #upload the newtable to the database
    print("Uploading new table to database.")
    df.to_sql(table_name, con)

    #close connection
    print("DONE. Closing database connection.")
    con.close()

if __name__ == '__main__':
    #path to database 
    db_name = '../data/db/rentnyc_db'                  
    #list of tables to merge
    table_list = [
        't20161030', 't20161102', 't20161103', 't20161104', 't20161106', 't20161107', 't20161108', 't20161109',
        't20161110', 't20161111', 't20161112', 't20161113', 't20161114', 't20161115', 't20161116', 't20161117',
        't20161118', 't20161119', 't20161120', 't20161121', 't20161122', 't20161123', 't20161124', 't20161125',
        't20161126', 't20161127', 't20161128', 't20161130', 't20161201', 't20161202', 't20161203', 't20161204',
        't20161205', 't20161206', 't20161209', 't20161211', 't20161212', 't20161213', 't20161215', 't20161216',
        't20161217', 't20161218', 't20161219', 't20161220', 't20161221', 't20161222', 't20161223', 't20161224',
        't20161225', 't20161226', 't20161230', 't20161231', 't20170101', 't20170102', 't20170103', 't20170104',
        't20170105', 't20170106', 't20170107', 't20170108', 't20170110', 't20170111']
    #name the new table.
    table_name = 'all_data'
    #run function
    main(db_name, table_list, table_name)
    