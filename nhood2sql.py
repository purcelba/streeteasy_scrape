#write program to add the nhood_borough lookup table to the sql database for use with the application.

import pandas as pd
import sqlite3
import numpy as np

def main(db_name, csv_name, table_name):
    """
    Reads in the csv file storing the name of all neighborhoods and the associated borough and 
    adds that information to a table in the SQLite database.
    
    Parameters
    ----------
    db_name, str
      - Name of the database.  Created if it doesn't already exist.
    csv_name, str
      - Name of the csv file containing the neighborhood (col1) and borough (col2) information.
    table_name, str
      - Name of the resulting table to be added to the database.  
    """
    #import as dataframe
    df = pd.read_csv(csv_file)
    #remove duplicate neighborhoods
    df = df.drop_duplicates(subset='Name')
    #connect to the database
    con = sqlite3.connect(db_name)
    #set up a cursor to the table, then use the execute command to query the table.
    c = con.cursor()
    #drop the table if it already exists
    c.execute("""
    DROP TABLE IF EXISTS %s;
    """ % (table_name))
    #create a table with neighborhood as the primary key
    c.execute("""
    CREATE TABLE %s (nhood TEXT PRIMARY KEY, borough TEXT)
    """ % (table_name))
    #loop over the dataframe and insert the data into the table
    for i in np.arange(np.shape(df)[0]):
        #insert row into the database
        c.execute("""
        INSERT INTO %s
        VALUES ("%s","%s");
        """ % (table_name, df.iloc[i]['Name'], df.iloc[i]['Borough']))
    #commit
    con.commit()
    con.close()
    
if __name__=='__main__':
    #set defaults
    db_name = 'rentnyc_db'                #database name
    csv_file = 'neighborhood_borough.csv' #csv file listing borough and neighborhoods
    table_name = "nhood_borough"          #name of the table to be added to database.
    #call main function
    main(db_name, csv_name, table_name)