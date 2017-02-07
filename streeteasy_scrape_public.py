"""Scrape rental data from streeteasy.com. Verified 2/6/2017.
"""
from bs4 import BeautifulSoup
import urllib
import pandas as pd
import numpy as np
import datetime
import re
import sys
import os


def print_err():
    """Print information about an error"""
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback_details = {
                        'filename': exc_traceback.tb_frame.f_code.co_filename,
                        'lineno': exc_traceback.tb_lineno,
                        'name': exc_traceback.tb_frame.f_code.co_name,
                        'type': exc_type.__name__,
                        'message': exc_value.message,  # or see traceback._some_str()
                        }
    del (exc_type, exc_value, exc_traceback)
    for key, val in traceback_details.items():
        print key, val


def main(**kwargs):
    """Loop over all rental listings on streeteasy.com. Format into a Pandas DataFrame
       and save them in a csv.  Note that the program will continue running if it encounters
       errors on a page.  The error will be printed and the program will continue to the next
       listing or page.  Last debugged 2/6/2017.

    Keyword arguments:
        max_pages: int, default 3000
            How many listings pages to cycle through?  If goal is to scrap all listings then
            this should be set arbitrarily high.  The program will exit when the maximum listing
            page is exceeded (usually around 2000).
        verbose: logical, default False
            If true, will print the values of parameters being scraped for each listing.  If false,
            will only list the data_id and the url.
        partial_save: int, default 2
            Program will create a subdirectory for partial saves of the scraped data every 'partial_save'
            pages.  This ensures that the entire dataset is not lost if the program is interrputed.
            Set to 0 to turn off partial saving.  Saving often is recommended and does not substantially
            increase run time.

    """

    #Default keyword arguments
    max_pages = kwargs.get("max_pages",3000)
    verbose = kwargs.get("verbose",False)
    partial_save = kwargs.get("partial_save",2)

    # set up prefix for links
    prefix = "http://streeteasy.com"

    # initialize a database
        #features
    basic = ["data_id", "scrape_date", "link", "address", "price", "sq_ft", "per_sq_ft",
             "rooms", "beds", "baths", "unit_type","neighborhood", "days_on_streeteasy", "realtor"]
    amenities = ["bike room", "board approval required", "cats and dogs allowed", "central air conditioning",
                "concierge", "cold storage", "community recreation facilities", "children's playroom",
                "deck", "dishwasher", "doorman", "elevator", "full-time doorman", "furnished", "garage parking",
                "green building", "gym", "garden", "guarantors accepted", "laundry in building", "live-in super",
                "loft", "package room", "parking available", "patio", "pets allowed", "roof deck", "smoke-free",
                "storage available", "sublet", "terrace", "virtual doorman", "washer/dryer in-unit", "waterview",
                "waterfront"]
    transport = ["A","C","E","B","D","F","M","G","L","J","Z",
                 "N","Q","R","1","2","3","4","5","6","7","S",
                 "LIRR","PATH"]
    col_list = basic + amenities + transport
        #initalize DataFrame
    df = pd.DataFrame(columns=col_list) #to save all results.
    df_temp = pd.DataFrame(columns=col_list) #for partial saves.

    # iterate over pages of listings
    for page in np.arange(1, max_pages):
        try:
            # display
            print "**********************************************"
            print "Page %d" % (page)
            print "**********************************************"

            # load the url for this listing page
            url = "http://streeteasy.com/for-rent/nyc?page=%d" % (page)
            r = urllib.urlopen(url)

            # parse with bs4
            soup = BeautifulSoup(r, "lxml")
            soup.prettify()

            # check for error message. Exit if the page does not exists.
            err_msg = soup.find(class_="error-message")
            if err_msg:
                print "Page does not exist.  Stopping."
                break

            # get all listings from this search page
            listings = soup.find_all("div", class_="item")

            # run partial save if requested
            if partial_save > 0 and page % partial_save == 0:
                print "*****Partial save*****"
                if not os.path.isdir('partial_save'):
                    os.makedirs('partial_save')
                try:
                    #write temporary dataframe to csv
                    df_temp.to_csv('partial_save/df_temp.csv')
                    #if save was successful append to actual df and save
                    df = df.append(df_temp, ignore_index=True)
                    df.to_csv('partial_save/' + str(datetime.date.today()) + '.csv')
                except:
                    #if save was unsuccessful. do not append
                    print "Error saving df_temp.  Discarding recent data."
                    print_err()

                # reset df_temp
                df_temp = pd.DataFrame(columns=col_list)  # for partial saves.

            # begin looping over listings on this page
            for element in listings:
                try:
                    # divider
                    print "======================"

                    #initialize dict
                    d = {}
                    for v in col_list:
                        d[v] = np.nan

                    # grab and print the listing number
                    match = re.search(r'data-id="(\d+)', str(element))
                    d['data_id'] = int(match.group(1))
                    print "data_id: " + str(d['data_id'])

                    # get the link for this listing
                    d['link'] = prefix + element.a["href"]
                    print d['link']

                    #save the date
                    d['scrape_date'] = str(datetime.date.today())

                    # load the html for the listing as a new soup
                    r = urllib.urlopen(d['link'])
                    soup = BeautifulSoup(r, "lxml")

                    # get the address
                    d['address'] = soup.find(class_="incognito").getText()

                    # check the price for this listing. The price string will come "bundled" with the
                    # price arrow and secondary_text, we will first check for those.  If found, we will
                    # strip them to keep only the price.  There is probably a more elegant way to do this.
                    # grab everything including price arrow, price, and secondary text
                    d['price'] = soup.find(class_="price").get_text(strip=True, separator='\t')
                    # check for a price arrow, if found, strip it
                    price_arrow = soup.find(class_="price").find(class_="price_arrow")
                    if price_arrow:
                        price_arrow = price_arrow.get_text()
                        d['price'] = d['price'].replace(price_arrow, '')
                        # check for secondary text, if found, strip it
                    secondary_text = soup.find(class_="price").find(class_="secondary_text")
                    if secondary_text:
                        secondary_text = secondary_text.get_text()
                        d['price'] = d['price'].replace(secondary_text, '')
                        # now strip the price, convert to int
                        d['price'] = int(d['price'].replace("$", '').replace(',', '').strip())
                    if verbose:
                        print "price = %d" % (d['price'])

                    # now get everything from the detail cells, assign to variable based on the
                    # text that is found.
                    detail_cell = soup.find(class_="details_info").find_all(class_="detail_cell")
                    for i in detail_cell:
                        temp = str(i.get_text())
                        if temp.find('bed') != -1:
                            d['beds'] = float(re.search(r'[\d.\d]+', temp).group())  # drop non-numeric and convert to float
                            if verbose:
                                print "beds = %2.1f" % (d['beds'])
                        elif temp.find('per ft') != -1:
                            d['per_sq_ft'] = int(temp.replace('$', '').replace(' per ft&sup2', ''))
                            if verbose:
                                print "per_sq_ft = %d" % (d['per_sq_ft'])
                        elif temp.find('ft') != -1 and temp.find('per') == -1:
                            d['sq_ft'] = int(temp.replace(',', '').replace('ft&sup2', ''))
                            if verbose:
                                print "sq_ft = %d" % (d['sq_ft'])
                        elif temp.find('room') != -1:
                            d['rooms'] = float(re.search(r'[\d.\d]+', temp).group())
                            if verbose:
                                print "rooms = %2.1f" % (d['rooms'])
                        elif temp.find('bath') != -1:
                            d['baths'] = float(re.search(r'[\d.\d]+', temp).group())
                            if verbose:
                                print "baths = %2.1f" % (d['baths'])

                    # get the unit type and neighboor hood from the nobreak cell.
                    nobreak = soup.find_all(class_="nobreak")
                    d['unit_type'] = nobreak[0].getText()
                    if verbose:
                        print "unit_type = %s" % (d['unit_type'])

                    d['neighborhood'] = nobreak[1].getText().replace('in ', '')
                    if verbose:
                        print "neighborhood = %s" % (d['neighborhood'])

                    # days on market
                    vitals = str(soup.find(class_="vitals top_spacer"))
                    days_on_streeteasy_temp = re.search(r'([\d.\d]+) days on StreetEasy', vitals)
                    if days_on_streeteasy_temp:
                        d['days_on_streeteasy'] = int(days_on_streeteasy_temp.group(1))

                    if verbose:
                        print "days_on_streeteasy = %d" % (d['days_on_streeteasy'])

                    # realtor company and agent
                    try:
                        d['realtor'] = soup.find(id="agent-promo").a
                        if d['realtor']:
                            d['realtor'] = d['realtor'].getText()
                            if verbose:
                                print "realtor = %s" % (d['realtor'])
                    except:
                        print "***Realtor not found, skipping."

                    # now check for amenities
                    amenities_str = str(soup.findAll(class_="amenities big_separator"))
                    for a in amenities:
                        match = re.search(a, amenities_str, re.IGNORECASE)
                        if match:
                            d[a] = 1
                        else:
                            d[a] = 0

                    # now check for transport
                    sub = soup.find(class_="transportation")
                    for s in sub:
                        for l in transport:
                            match = re.search('"sub_icon line_%s"' % (l), str(s))
                            if match:
                                distance = re.search(r'<b>(.+)</b>', str(s)).group(1)                       # text describing the distance to stop
                                dist_num = float(re.search(r'([0-9]+.[0-9]+|[0-9]+)', distance).group(1))   # get the numeric distance value
                                ft_check = re.search(r'feet', distance)                                     # if in feet, convert to miles
                                if ft_check:
                                    dist_num = dist_num * 0.000189394
                                # only save the shortest distance
                                if (d[l] is np.nan) or (dist_num < d[l]):
                                    d[l] = dist_num


                    # append to DataFrame
                    df_temp = df_temp.append(d, ignore_index=True)


                except:
                    print "Error on page %d" % (page)
                    print_err()
                    continue

        except:
            print "Error on page %d" % (page)
            print_err()
            continue


    #save final df
    print "DONE.  Saving..."
    try:
        # write temporary dataframe to csv
        df_temp.to_csv('partial_save/df_temp.csv')
        # if save was successful append to actual df and save
        df = df.append(df_temp, ignore_index=True)
        df.to_csv(str(datetime.date.today()) + '.csv')
    except:
        # if save was unsuccessful. do not append
        print "Error saving df."
        print_err()

    #exit
    return df

if __name__ == '__main__':

    #defaults
    main(max_pages=2,verbose=True,partial_save=1)