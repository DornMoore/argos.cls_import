#!/usr/bin/env python

"""
cls_import.py:
Script reads through CLS data downloaded from https://argos-system.cls.fr/argos-cwi2/login.html 
and imports those data to our ICF database.

The new data inserted into the argos_ptt data table on the mapfeeder database.
TODO: 
* Add a notification to the database slack channel on successful import.
* Add a notification email on failed import.


"""

import pandas as pd
from sqlalchemy import create_engine
import geoalchemy2 as ga  # to handle geometries
import urllib
import os
import secrets as s  # secrets.py file
from getArgosData import getPlatforms, getCsv


__author__ = "Dorn Moore, International Crane Foundation"
__credits__ = ["Dorn Moore"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Dorn Moore"
__email__ = "dorn@savingcranes.org"
__status__ = "Development"

# Setup the connection to the Database with SQL Alchemy
db_string = s.db_string
db = create_engine(db_string, executemany_mode='values',
                   executemany_values_page_size=10000)

# Get the list of platforms from the CLS website using the getPlatforms function in getArgosData.py
platforms = getPlatforms(s.argosUser, s.argosPass, s.argosProgram)
for platform in platforms:
    argos_csv = getCsv(s.argosUser, s.argosPass, platform)
    # save the csv file to the local directory temporarily
    with open(str(platform)+'.csv', 'w') as f:
        for row in argos_csv:
            f.write(row + '\n')

    # print(str(platform)+'.csv file successfully written')

    # set the csv file name
    csvFile = str(platform)+'.csv'
    # read the csv file into a pandas dataframe - could be skipped if we just read the csv file directly into the dataframe
    remoteData = pd.read_csv(
        csvFile, header=0, skip_blank_lines=True, index_col=False)

#  Create a dictionary of the dataframe columns that we want to import to our database.
#  The key is the column name in the database and the value is the column name in the csv file.
    columnRenames = {"platformId": "ptt_id",
                     "duration": "average_ti",
                     "longitude": "long1",
                     "latitude": "lat1",
                     "locationClass": "fix",
                     "frequency": "frequency"
                     }

    # Let's convert the comlumns that can change easily
    remoteData.rename(columns=columnRenames, inplace=True)

    # Now we'll split the timestamp in 'Loc. date' to a date and time
    remoteData['date'] = pd.to_datetime(remoteData['bestMsgDate']).dt.date
    remoteData['time'] = pd.to_datetime(remoteData['bestMsgDate']).dt.time
    # change NAN values to empty string
    remoteData.fillna('', inplace=True)
    # print(remoteData)
    # remove the rows without coordinates (fix is our proxy for coordinates)
    remoteData = remoteData[remoteData['fix'].str.strip().astype(bool)]

    # set remoteData fields to strings for easy comparison
    remoteData = remoteData.astype(str)

    # get all of the data from the database and put it into a dataframe
    localData = pd.read_sql_table('argos_ptt', db, schema='tracking')

    # Before we start, we need to make sure the datframes have the same columns
    # Create a list of the fields in common
    cols = remoteData.columns.intersection(localData.columns)

    # finally, We'll keep only the columns in both dataframes
    remoteData = remoteData[cols]
    localData = localData[cols]
    # set localData fields to strings for easy comparison
    localData = localData.astype(str)

    # Filter Records from new data that's already in the DB
    newData = (
        remoteData.merge(localData,
                         on=['ptt_id', 'date', 'time', 'fix'],
                         how='left',
                         indicator=True)
        .query('_merge == "left_only"')
        .drop(columns='_merge')
    )
    newData = newData.loc[:, ~newData.columns.str.endswith('_y')]
    newData.columns = newData.columns.str.replace('_x', '')

    if newData.empty:
        print("No new data to import")
    else:
        try:
            print("Importing %s records to the database from ptt_id %s" %
                  (len(newData.index), platform))
            # print(newData[0:5])
            # insert the new data into the database table tracking.argos_ptt in batches of 10000
            newData.to_sql(name='argos_ptt', con=db, schema='tracking',
                           if_exists='append', index=False, chunksize=10000)
        except Exception as e:
            print("Error importing new data: %s" % e)

    # Remove the csv file
    os.remove(csvFile)
