"""
Read in the raw data from a single .csv file (one day of data).
Clean the data.
Format the data for the SQLite table.
Upload to the rentnyc_db database.
"""


import pandas as pd
import sqlite3
import numpy as np


def main(db_name, data_directory, csv_file, borough_file):
    """Will load a .csv produced from the streeteasy_scrape_public.py function, perform some additional formatting
    that was not handled in the web scraper function, and save the results in a local SQLite database.  
    
    The following operations are performed:
        - The "per_sq_ft" column is eliminated.  It is a linear combination of price and sq_ft.
        - Missing values are encoded as -1
        - The "data_id" column will be set as the primary key.
        - Blank rows of the 'beds' column will be converted to 0 to imply a studio apartment.
        - Missing transportation values (subway lines/trains) are changed to 0 to imply absence.
        - A new column for borough is created based on the neighborhood_borough.csv file.
        - The apostrophe in Hell's Kitchen is removed for simpler string calling.
        - Prices that are listed with "Last listed as ..." are converted to actual prices.
        - Realtors listed as "View original listing" is changed to a missing value.
        - Duplicated data_ids are removed.
        - Some outliers are dropped (>15 rooms and price <20000, listings with >30 beds).
        
    Parameters
    ----------
      db_name: str
        - name of the database in which to save the table.  If it exists, the table will be added.
          If it does not exist, it will be created.
      data_directory: str
        - directory in which to find the .csv data file.
      csv_file: str
        - name of the csv file to be loaded.
      borough_file: str
        - name and path of the csv file containing the borough associated with each neighborhood.
        
    """


    #import as dataframe
    print "Reading csv from %s" % (data_directory + csv_file)
    df = pd.read_csv(data_directory + csv_file)
    print "Done.\n"

    #check for missing columns, if found, pad with nans
    col_list = ["data_id", "scrape_date", "link", "address", "price", "sq_ft", "per_sq_ft",
                "rooms", "beds", "baths", "unit_type", "neighborhood", "days_on_streeteasy", "tor",
                "bike room", "board approval required", "cats and dogs allowed", "central air conditioning",
                "concierge", "cold storage", "community recreation facilities", "children's playroom",
                "deck", "dishwasher", "doorman", "elevator", "full-time doorman", "furnished", "garage parking",
                "green building", "gym", "garden", "guarantors accepted", "laundry in building", "live-in super",
                "loft", "package room", "parking available", "patio", "pets allowed", "roof deck", "smoke-free",
                "storage available", "sublet", "terrace", "virtual doorman", "washer/dryer in-unit", "waterview",
                "waterfront", "A", "C", "E", "B", "D", "F", "M", "G", "L", "J", "Z",
                "N", "Q", "R", "1", "2", "3", "4", "5", "6", "7", "S",
                "LIRR", "PATH"]
    for col in col_list:
        if col not in df.columns:
            df[col] = np.nan

    #rename columns to remove spaces and special characters
    old_col = ["bike room","board approval required","cats and dogs allowed","central air conditioning","cold storage",
               "community recreation facilities", "children's playroom", "full-time doorman", "garage parking",
               "green building","guarantors accepted", "laundry in building", "live-in super", "package room",
               "parking available", "pets allowed","roof deck","smoke-free","storage available","virtual doorman",
               "washer/dryer in-unit", "A", "C", "E", "B", "D", "F", "M", "G", "L", "J", "Z",
                "N", "Q", "R", "1", "2", "3", "4", "5", "6", "7", "S"]
    new_col = ["bike_room","board_approval_required","cats_and_dogs_allowed","central_air_conditioning","cold_storage",
               "community_recreation_facilities","children_playroom","full_time_doorman","garage_parking",
               "green_building","guarantors_accepted","laundry_in_building","live_in_super","package_room",
               "parking_available","pets_allowed","roof_deck","smoke_free","storage_available","virtual_doorman",
               "washer_dryer_in_unit","line_A","line_C","line_E","line_B","line_D","line_F","line_M","line_G","line_L",
               "line_J","line_Z","line_N","line_Q","line_R","line_1","line_2","line_3","line_4","line_5","line_6",
               "line_7","line_S"]
    for i in range(len(new_col)):
        df.rename(columns = {old_col[i]: new_col[i]}, inplace = True)


    #########
    #Clean data
    #########

    #eliminate unnamed column
    if 'Unnamed: 0' in df:
        df = df.drop('Unnamed: 0',1)

    #eliminate per sq ft (linearly related to price)
    df = df.drop('per_sq_ft',1)

    #change missing beds values to zero to imply studio.
    df.loc[df['beds'].isnull(),'beds'] = 0

    #change missing subway lines values to zero to imply line is absent.
    sub_list = ["line_A","line_C","line_E","line_B","line_D","line_F","line_M","line_G","line_L",
               "line_J","line_Z","line_N","line_Q","line_R","line_1","line_2","line_3","line_4","line_5","line_6",
               "line_7","line_S","LIRR","PATH"]
    for s in sub_list:
        df.loc[df[s].isnull(),s] = 0

    #replace remaining missing values with a code
    df = df.fillna(value=-1)

    #drop duplicate data_id.
    #note that if we are looking at longitudinal plots, we want to include a second column for scrape_date
    df = df.drop_duplicates(['data_id'])

    #Eliminate the data point with > 15 rooms with the price <20000.
    #In pandas, we delete rows by specifying rows to keep
    df = df.loc[~((df['rooms'] > 15) & (df['price'] < 20000))]

    #drop listings with >=30 beds
    df = df.loc[df['beds'] < 30]

    #change realtor "View original listing" to a missing value
    df.loc[df['realtor'] == "View original listing",'realtor'] = -1
    df.loc[df['realtor'] == "View original listing ",'realtor'] = -1


    #########
    #Format for SQL database
    #########

    #establish a connection to a sql database, if it does not already exist, it is created
    #note that rentnyc is the name of the database and it can have multiple internal tables
    print "Connecting to %s database." % (db_name)
    con = sqlite3.connect(db_name)
    print "Done.\n"
    print "Begin formatting data."

    #set up a cursor to the table, then use the execute command to query the table.
    c = con.cursor()

    #modify the csv file name to get the temporary table name
    table_name_tp = 't' + csv_file.replace('.csv','_tp').replace('-','')    #temporary table to import dataframe without primary key
    table_name = table_name_tp.replace('_tp','')            #final table that will add a primary key

    #drop the table_name_tp if it already exists
    c.execute("""
    DROP TABLE IF EXISTS %s;
    """ % (table_name_tp))

    #add the dataframe as a table in the database
    df.to_sql(table_name_tp,con) #create a table named 20161005

    #the to_sql method does not allow us to add a primary key.  to do this, we must create
    #a new table and copy the data
        #drop the table if it already exists
    c.execute("""
    DROP TABLE IF EXISTS %s;
    """ % (table_name))
        #create the table
    c.execute("""
    CREATE TABLE %s (
        data_id INTEGER PRIMARY KEY, scrape_date TEXT, link TEXT, address TEXT, price REAL, sq_ft REAL,
        rooms INTEGER, beds INTEGER, baths INTEGER, unit_type , neighborhood TEXT, days_on_streeteasy INTEGER,
        realtor INTEGER, bike_room INTEGER, board_approval_required INTEGER, cats_and_dogs_allowed INTEGER,
        central_air_conditioning INTEGER, concierge INTEGER, cold_storage INTEGER, community_recreation_facilities INTEGER,
        children_playroom INTEGER, deck INTEGER, dishwasher INTEGER, doorman INTEGER, elevator INTEGER, full_time_doorman INTEGER,
        furnished INTEGER, garage_parking INTEGER, green_building INTEGER, gym INTEGER, garden INTEGER, guarantors_accepted INTEGER,
        laundry_in_building INTEGER, live_in_super INTEGER, loft INTEGER, package_room INTEGER, parking_available INTEGER,
        patio INTEGER, pets_allowed INTEGER, roof_deck INTEGER, smoke_free INTEGER, storage_available INTEGER, sublet INTEGER,
        terrace INTEGER, virtual_doorman INTEGER, washer_dryer_in_unit INTEGER, waterview INTEGER, waterfront INTEGER,
        line_A REAL, line_C REAL, line_E REAL, line_B REAL, line_D REAL, line_F REAL, line_M REAL, line_G REAL, line_L REAL,
        line_J REAL, line_Z REAL, line_N REAL, line_Q REAL, line_R REAL, line_1 REAL, line_2 REAL, line_3 REAL, line_4 REAL,
        line_5 REAL, line_6 REAL, line_7 REAL, line_S REAL, LIRR REAL, PATH REAL)
    """ % (table_name))

    #delete duplicates in the temporary table before adding to the new table
    c.execute("""
    DELETE FROM %s
    WHERE rowid NOT IN
        (
        SELECT MAX(rowid)
        FROM %s
        GROUP BY data_id
        );
    """ % (table_name_tp,table_name_tp))

    #join the old table with the new one
    c.execute("""
    INSERT INTO %s
        (data_id, scrape_date, link, address, price, sq_ft,
        rooms, beds, baths, unit_type, neighborhood, days_on_streeteasy, realtor,
        bike_room, board_approval_required, cats_and_dogs_allowed,
        central_air_conditioning, concierge, cold_storage, community_recreation_facilities,
        children_playroom, deck, dishwasher, doorman, elevator, full_time_doorman,
        furnished, garage_parking, green_building, gym, garden, guarantors_accepted,
        laundry_in_building, live_in_super, loft, package_room, parking_available,
        patio, pets_allowed, roof_deck, smoke_free, storage_available, sublet,
        terrace, virtual_doorman, washer_dryer_in_unit, waterview, waterfront,
        line_A, line_C, line_E, line_B, line_D, line_F, line_M, line_G, line_L,
        line_J, line_Z, line_N, line_Q, line_R, line_1, line_2, line_3, line_4,
        line_5, line_6, line_7, line_S, LIRR, PATH)
    SELECT
        data_id, scrape_date, link, address, price, sq_ft,
        rooms, beds, baths, unit_type, neighborhood, days_on_streeteasy,realtor,
        bike_room, board_approval_required, cats_and_dogs_allowed,
        central_air_conditioning, concierge, cold_storage, community_recreation_facilities,
        children_playroom, deck, dishwasher, doorman, elevator, full_time_doorman,
        furnished, garage_parking, green_building, gym, garden, guarantors_accepted,
        laundry_in_building, live_in_super, loft, package_room, parking_available,
        patio, pets_allowed, roof_deck, smoke_free, storage_available, sublet,
        terrace, virtual_doorman, washer_dryer_in_unit, waterview, waterfront,
        line_A, line_C, line_E, line_B, line_D, line_F, line_M, line_G, line_L,
        line_J, line_Z, line_N, line_Q, line_R, line_1, line_2, line_3, line_4,
        line_5, line_6, line_7, line_S, LIRR, PATH
    FROM %s;
    """ % (table_name,table_name_tp))

    #delete the temporary table
    c.execute("""
    DROP TABLE %s;
    """ % (table_name_tp))


    #We are now finished creating our new table in the database.
    #We will now clean up some of the neighborhood names and add a borough column

    #insert a new column for borough
    c.execute("""
    ALTER TABLE "%s"
    ADD "borough" TEXT;
    """ % (table_name))

    #eliminate the apostrophe in Hell's Kitchen
    c.execute("""
    UPDATE '%s'
    SET neighborhood = 'Hells Kitchen'
    WHERE neighborhood = "Hell's Kitchen"
    """ % (table_name))
    con.commit()

    #get a list of all neighborhoods
    c.execute("""
    SELECT DISTINCT neighborhood
    FROM "%s";
    """ % (table_name))
    nhood_tups = c.fetchall()

    #conver to list of strings
    nhood_list = [str(i[0]) for i in nhood_tups]

    #load the borough list
    df_borough = pd.read_csv(borough_file)

    #now put it all together with sql commands.
    for nhood in nhood_list:
        borough = df_borough[df_borough['Name'] == nhood]['Borough'].max()
        query = "UPDATE %s SET borough = '%s' WHERE neighborhood = '%s';" % (table_name,borough,nhood)
        c.execute(query)

    #Find prices that say "Last listed as..." and convert to actual price
        #query all rows with prices starting "Last listed ...", save as df
    df_tp = pd.read_sql("""
    SELECT *
    FROM %s
    WHERE price LIKE "Last listed %%";
    """ % (table_name),con)
        #loop over rows of the df
    for i in np.arange(np.shape(df_tp)[0]):
        #reformat the price
        new_price = df_tp['price'][i].replace("Last listed at\t$","").replace(",","")
        #update the sql table
        data_id_tp = df_tp['data_id'][i]
        c.execute("""
        UPDATE %s
        SET price=%s
        WHERE data_id=%s;
        """ % (table_name,new_price,data_id_tp))

    #Commit the changes
    con.commit()

    # print the first ten rows as reality check
    print "Data formatted. Printing first ten rows as reality check.\n"
    c.execute("""
    SELECT * FROM %s;
    """ % (table_name))
    x =  c.fetchall()
    for i in x[0:10]:
        print i

    #Finished! We now have a table in our database with data_id set as the primary key!
    con.close()

if __name__ == '__main__':
    #set variables
    db_name = '../data/db/rentnyc_db'    # database name
    data_directory = '../data/csv_raw/'  # list the dataframe csv files
    csv_file = '2017-01-11.csv'          # try analyzing individual files. note: data older than 2016-10-30 are missing variables.
    borough_file = '../data/misc/neighborhood_borough.csv'
    #run function
    main(db_name, data_directory, csv_file, borough_file)
