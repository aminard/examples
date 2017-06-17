#!/usr/bin/python
# -*- coding: utf-8 -*-

# Description: ETL Salesforce BVSite data and MA PRR client data into sci_entry.Clients
# Date written: March 2, 2017
# 
# What is inserted into Clients table:
# Enabled PRR clients, excluding Syndication 1.0 client instances
# and excluding UCS blacklist clients, that have a BVSite tied to an Account that is 
# of type Customer, Network Customer, Cross-Sell, or Prospect, or is the PowerReviews account.
#
# What's new: Uses SFDC REST API (not SOAP), uses common library for DB connections, 
# uses pandas for data manipulation (not dictionaries), improved legibility

import logging
import sys
sys.path.append('/var/www/scii_jobs/scripts/')
sys.path.append('/var/www/scii_jobs/scripts/clients_table/')
import time
import queries
import pandas
import numpy
from collections import OrderedDict
from common import database, emohelper
from datetime import datetime
#import json

# Disable false positive warning
pandas.options.mode.chained_assignment = None

def get_sfdc_data():

	# Connect to SFDC and run SOQL query.
	logging.info('Connecting to SFDC...')
	sf = database.connect_sfdc()
	query = queries.sfdc_data
	logging.info('Running query on SFDC...')
	results = database.run_query_sfdc(sf, query)
	size = results['totalSize']
	logging.info('Retrieved %s records.', size)
	#print json.dumps(results, indent=2)
	
	# Parse results dictionary and store in DataFrame.
	data = [get_record_values(record) for record in results['records']]
	data = pandas.DataFrame.from_records(data)
	
	# Lowercase 'Name' so it can be merged with MA data later.
	data.Name = data.Name.str.lower()

	return data

def get_record_values(rec, parentkey=None):

	# Parse the nested OrderedDict that is returned from SOQL query,
	# and create a flattened dictionary for pandas to read into DataFrame.
	# Stores and uses parent key in case of duplicate child keys.
	# e.g. Account__r.Parent.Name and Account__r.Client_Partner2__c.Name will
	# assign 'Parent' and 'Client_Partner2__c' as keys for their children's 'Name' values
	# so that the final 'Name' is not overwritten during dict update, and we get all
	# the keys we need. 
	result = {}
	for key, val in rec.items():
		if key == 'attributes': continue
		if isinstance(val, OrderedDict):
			parentkey = key
			# Recursive call
			result.update(get_record_values(val, parentkey))
		else:
			if isinstance(val, str):
				val = val.encode('utf-8')
			if parentkey:
				result[parentkey] = val
				parentkey = None
			else:
				result[key] = val

	return result

def get_ma_data():

	try:
		# Initialize list to store DataFrames for each MA cluster.
		data = []
		for cluster in range(1, 8):
			query = queries.ma_clients
			query = query.format(cluster)
			temp = database.execute_query(query, db='ma', cluster=cluster)
			data.append(temp)
			logging.info('Got clients on MA c%s.', cluster)
		
		# Concatenate all 7 clusters into single DataFrame.
		data = pandas.concat(data)

		return data

	except Exception as e:
		logging.exception(e)
		sys.exit(1)

def merge_data(left, right):

	try:
		# Left join MA clients with Salesforce data so that we get all client instances,
		# even those without a BVSite.
		data = pandas.merge(left, right, how='left', on=['Name'])
		logging.info('Merged MA and SFDC data.')

		return data

	except Exception as e:
		logging.exception(e)
		sys.exit(1)

def exclude_blacklist(df):

	try:
		logging.info('Connecting to EmoDB REST API...')
		helper = emohelper.EmoHelper()
		logging.info('Getting blacklist...')
		blacklist = helper.get_blacklist()

		if not blacklist:
			logging.error("Failed to get blacklist!")
			sys.exit(1)

		# Remove clients from DataFrame that are in blacklist
		before = len(df)
		df = df[~df.Name.isin(blacklist)]
		after = len(df)
		logging.info('Blacklist filtered %s records.', (before-after))
		
		return df

	except Exception as e:
		logging.exception(e)
		sys.exit(1)

def truncate_table():

	try:
		# Open connection to dataportal.sci_entry and truncate the table.
		logging.info('Truncating table...')
		query = queries.truncate
		connection = database.get_creds_and_connect_sa_sci()
		cursor = connection.cursor()		
		cursor.execute(query)
		connection.commit()
		logging.info('Truncated sci_entry.Clients.')

	except Exception as e:
		connection.rollback()
		logging.exception(e)
		sys.exit(1)

	finally:
		cursor.close()
		connection.close()

def insert_data(df):

	PWR_ACCOUNT = '00150000018geuWAAQ'
	CRAP_ACCOUNT = '0015000001A9V8KAAV'
	TYPES = ['Customer', 'Network Customer', 'Cross-Sell', 'Prospect']

	# Additional filtering on DataFrame before insertion.
	before = len(df)
	df = df[df.Account__c != CRAP_ACCOUNT]
	after = len(df)
	logging.info('Crap filtered %s records.', (before-after))

	# Update Vertical for PWR BVSites
	df.loc[df.Account__c == PWR_ACCOUNT, 'Vertical__c'] = 'PowerReviews'

	try:
		# Open connection to dataportal.sci_entry
		connection = database.get_creds_and_connect_sa_sci()
		cursor = connection.cursor()

		# Initialize some counters for logging
		inserted_count = 0
		skip_count = 0

		# Insert each row of DataFrame into sci_entry.Clients
		logging.info('Inserting data...')

		for index, row in df.iterrows():
			query = queries.insert
			values = (int(row.ClientID),
				row.Name,
				int(row.Cluster),
				row.Netsuite_Internal_ID__c,
				row.Account__r,
				row.Parent,
				row.Type,
				row.Vertical__c,
				row.Industry,
				row.CS_Vertical__c,
				row.Client_Success_Director2__r,
				row.Owner,
				row.Booked_ASF_Netsuite2__c,
				row.Netsuite_Account_Name__c,
				row.SFDC_BV_Site_ID_18__c,
				row.BillingCountry,
				row.ShippingCountry,
				row.RAD_Score__c,
				row.Client_Partner2__r,
				int(0),
				row.CS_Segment__c,
				row.Region__c,
				row.Sub_Region__c,
				row.Country_Code__c,
				row.Company_Size__c,
				row.Account_Classification__c,
				#None,
				row.Website,
				row.EngagementTier__c,
				row.Account_Tier__c,
				#None,
				row.Platform__c,
				row.Receive_Brand_Answers__c, 
				row.Receive_Brand_Rev_Responses__c, 
				row.Receive_Syndication_Curations__c, 
				row.Receive_Syndication_Q_A__c, 
				row.Receive_Syndication_Reviews__c, 
				row.Send_Syndication_Curations__c, 
				row.Send_Syndication_Q_A__c, 
				row.Send_Syndication_Reviews__c
				)

			# Final filtering occurs during insertion.
			if (row.Type in TYPES) or (row.Account__c == PWR_ACCOUNT):
				cursor.execute(query, values)
				inserted_count += 1
			else:
				skip_count += 1

		connection.commit()
		logging.info('Inserted %s rows.', inserted_count)
		logging.info('Skipped %s rows.', skip_count)

	except Exception as e:
		logging.exception(e)
		logging.info(row)
		logging.info(query)
		connection.rollback()
		sys.exit(1)

	finally:
		cursor.close()
		connection.close()

if __name__ == '__main__':

	startTime = time.time()

	# Configure logging
	today = datetime.now().strftime('%Y-%m-%d')
	filepath = '/var/www/scii_logs/clients_table/clients_table_'+today+'.log'
	logging.getLogger('').handlers = []
	logging.basicConfig(level=logging.INFO,
						format='%(asctime)s %(levelname)-4s %(message)s',
						datefmt='%Y-%m-%d %H:%M:%S',
						filename=filepath,
						filemode='w')
	console = logging.StreamHandler()
	console.setLevel(logging.INFO)
	logging.getLogger('').addHandler(console)
	logging.info('Starting Clients table update!')

	# Get all the data, merge it, filter out UCS blacklist clients
	sf_data = get_sfdc_data()
	ma_data = get_ma_data()
	merged = merge_data(ma_data, sf_data)
	merged = exclude_blacklist(merged)

	# Fill NA/NaN with None so that an actual NULL value (instead of empty string or string 'NULL') is inserted into database 
	final = merged.where((merged.notnull()), None)

	# Export report of client instances that are missing a SF BVSite (i.e. have no SF data)
	missing_bvsites = final[~final.ix[:,3:].notnull().any(axis=1)]
	missing_bvsites.ix[:,:3].to_csv('/var/www/scii_logs/clients_table/MISSING_BVSITES_'+today+'.csv', encoding='utf-8', index=False)
	logging.info('Created report of missing BV Sites.')

	# Final insert operation
	truncate_table()
	insert_data(final)

	endTime = time.time()
	logging.info('Done! Script took %s minutes to run.', round((endTime-startTime)/60, 1))
