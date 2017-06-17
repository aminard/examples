# ETL from MySQL and Salesforce REST API

## Description
Pull core data from master MySQL databases, merge with important contextual data from Salesforce.com, and store on our own EC2 instance MySQL database for further use in downstream automated reporting applications.

## Filters
* Enabled clients only
* Exclude legacy syndication instances
* Exclude test instances and non-customer Account types in SFDC

## Enhancements (over v1)
* Upgraded to SFDC REST API instead of SOAP
* Upgraded to use common library for database connections
* Upgraded to pandas for data manipulation, instead of dictionaries
* Improved code legibility
* Runtime reduced from 15 min to 1 min
