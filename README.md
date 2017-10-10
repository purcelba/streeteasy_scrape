# streeteasy_scrape

Python code for scraping real-estate data from a popular rental listings web page.

## Important
As of 2/20/2017, Streeteasy.com has blocked web scraping (https://www.distilnetworks.com/).  I'm keeping this repository here to archive the [existing database](https://github.com/purcelba/streeteasy_scrape) and code from which it was generated. Consider https://rentlogic.com/ as a potential alternative source for rental listing data.


## About
This repository contains a Python script for scraping and formatting rental listings
from the popular NYC rental listings web page www.streeteasy.com. 

The core function is streeteasy_scrape_public.py.  By default, it will loop over all listings on the website producing ~27,000 listings on any given day. The results are saved in .csv format.  An example data set, 2016-12-20.csv, is included in the Data directory. 

## Data

The following variables are formatted and saved in a csv file:
- **data_id:** a unique integer identifying each listing
- **scrape_date:** the date on which the data were collected
- **link:** the listing url
- **address:** the street address of the listing 
- **price:** monthly rental price (USD)
- **sq_ft:** the area of the listing in square feet.  Blank if not found.
- **per_sq_ft:** price per square foot (USD). Blank if not found.
- **rooms:** total number of rooms. Blank if not found.
- **beds:** total number of bedrooms. Blank if not found.
- **baths:** total number of bathrooms
- **unit_type:** type of listing (Rental Unit, Condo, Multi-family, Town house, Co-op, Building, House)
- **neighborhood:** NYC neighborhood name
- **days_on_streeteasy:** number of days that the listing has been on streeteasy. Blank if not found.
- **realtor:** name of realtor. Blank if not found.
- **amenities:** The following columns will return 1 if the amenity is listed, 0 if not.
    - bike room, board approval required, cats and dogs allowed, central air conditioning,concierge, cold storage, community recreation facilities, children's playroom,deck, dishwasher, doorman, elevator, full-time doorman, furnished, garage parking, green building, gym, garden, guarantors accepted, laundry in building, live-in super, loft, package room, parking available, patio, pets allowed, roof deck, smoke-free, storage available, sublet, terrace, virtual doorman, washer/dryer in-unit, waterview, waterfront
- **transportation:** The following columns will return the distance (in miles) to the following subway lines and trains.  Distances under  50 ft are reported as 50ft.  Blanks (NaNs) indicate that this train is approximately >1.8 mi away.
    - Line A, Line C, Line E, Line B, Line D, Line F, Line M, Line G, Line L, Line J, Line Z, Line N, Line Q, Line R, Line 1, Line 2, Line 3, Line 4, Line 5, Line 6, Line 7, Line S, LIRR, PATH

### Example Single-Day Data Set

The Data directory contains an example .csv file, 2016-12-20.csv, which is the result of a single complete scrape of all listings on StreetEasy.com on a single day.  The figure below shows a screenshot of the data.  Download the .csv file for the complete output.

<p align="center">
<img src="data_screen_shot.png" width=100% /><br>
</p>


## Additional Formatting

Two functions are included to transfer and format the data into a SQLite local database.  
**csv2sql.py** performs additional formatting for ease of use and adds the resulting table to a SQLite local database.  
**mergeSQL.py** merges tables collected on different days within the SQLite database into a single table after removing duplicates.  An additional column indicating the *borough* is added.

## Formatted Multiple-Day Data Set

The Data directory contains a formatted SQLite database, *streeteasy_db*.  The data were scraped between 11/02/2016 and 1/31/2017 (~63,000 unique listings), added to the database using csv2sql.py, and formatted using mergeSQL.py.  The last ten days of data are combined into a table, *test_data*, and the remainder are in a separate table, *train_data*, to facilitate modeling and validation.  Code for analzying and modeling rental prices in this database can be found in the [streeteasy_model](https://github.com/purcelba/streeteasy_model) repository.  This is the back-end database for the  [RentNYC](http://www.bradenpurcell.net/rentapp/) web-application.



