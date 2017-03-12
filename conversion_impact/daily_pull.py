#!/usr/bin/env python

import glob
import os
import sys
import time

import psycopg2
import pandas

from datetime import datetime, timedelta

def get_credentials():

	user = os.environ.get('MAGPIE_REDSHIFT_USER')
	pwd = os.environ.get('MAGPIE_REDSHIFT_PWD')

	return user, pwd

def merge_data():
	# Join the intermediate CSVs (in ./csv/ directory) from each query into single CSV.

	print "Merging columns..."

	mergedDF = pandas.DataFrame()
	files = glob.glob("csv/"+client+"_*.csv")

	# Initialize the final dataframe with first two CSV files (one being Total Visitors, to avoid missing any days).
	totalIndex = files.index("csv/"+client+"_total_visitors.csv")
	t = files.pop(totalIndex)
	tdf = pandas.read_csv(t)

	f = files.pop()
	df = pandas.read_csv(f)
	mergedDF = pandas.merge(tdf, df, on=['client','date'], how='left')

	# Continue with rest of files sequentially.
	while files:
		f = files.pop()
		df = pandas.read_csv(f)
		mergedDF = pandas.merge(mergedDF, df, on=['client','date'], how='left')

	mergedDF = mergedDF.fillna(0)
	print mergedDF
	mergedDF.to_csv("results/"+client+"_results.csv", index=False)

def read_queries():
	# Read queries from .sql file (query name and query strings separated with ;), and create a dictionary

	with open('queries.sql', 'r') as f:
		contents = f.read()

	queries = contents.split(";")
	for i in range(len(queries)):
		queries[i] = queries[i].strip("\n")

	queryDict = dict(zip(*[iter(queries)]*2))

	return queryDict

def run_query(connection, query):	
	# Execute queries sequentially

	query = query.format(lastDate)
	print "\nRunning query: ", queryName, lastDate
	#print query
	cursor = connection.cursor()

	try:
		cursor.execute(query)
		results = cursor.fetchall()
		columnNames = [i[0] for i in cursor.description]

		if not results:
			print "\nNull results!\n"

		return results, columnNames

	except psycopg2.Error as e:
		print "Error: ", e
		sys.exit()

	finally:
		if cursor:
			cursor.close()
			#print "Cursor closed."

def get_dates():
	# Get dates from user

	client = raw_input("Enter client name:  ")
	startDate = raw_input("Enter start date (YYYY-MM-DD):  ")
	endDate = raw_input("Enter end date (YYYY-MM-DD):  ")

	startDate = datetime.strptime(startDate, "%Y-%m-%d").date()
	endDate = datetime.strptime(endDate, "%Y-%m-%d").date()

	return client, startDate, endDate

if __name__ == "__main__":

	os.chdir(sys.path[0])

	if not os.path.exists("./csv"):
		os.mkdir("./csv")

	if not os.path.exists("./results"):
		os.mkdir("./results")

	client, startDate, endDate = get_dates()
	startTime = time.time()
	queries = read_queries()

	try:
		# Connect to Redshift
		usr, pwd = get_credentials()
		print "\nConnecting to Redshift..."
		connection = psycopg2.connect(database='magpie', user=usr, password=pwd, host='redshift.mag.bazaarvoice.com', port=5439)
		if connection.closed == 0:
			print "Connected."

		# Run each query, one day at a time, then concatenate daily rows into one dataframe and write to CSV.
		for queryName, query in queries.iteritems():
			lastDate = startDate
			dailyResults = []
			while lastDate <= endDate:
				q = query.format(client, lastDate)
				results, header = run_query(connection, q)
				df = pandas.DataFrame(results, columns=header)
				print df
				dailyResults.append(df)
				lastDate += timedelta(days=1)
			print "\nConcatenating daily results...\n"
			finalResults = pandas.concat(dailyResults)
			print finalResults
			print "\nWriting to CSV...\n"
			finalResults.to_csv("csv/"+client+"_"+queryName+".csv", index=False)

	except Exception as e:
		print "Error: ", e
		
	finally:
		connection.close()
		print "Connection closed.\n"

	try:
		merge_data()
	except Exception as e:
		print "Error: ", e
	
	endTime = time.time()
	print "\nDone.\nScript took", round((endTime - startTime) / 60, 1), "minutes to run.\n"
