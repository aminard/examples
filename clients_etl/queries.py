#!/usr/bin/python
# -*- coding: utf-8 -*-

# IMPORTANT: Any field with duplicate keys need to go at end of SELECT in SOQL query.

sfdc_data = """
	SELECT 
		Account__c,
		Name, 
		SFDC_BV_Site_ID_18__c,
		Receive_Brand_Answers__c,
		Receive_Brand_Rev_Responses__c,
		Receive_Syndication_Curations__c,
		Receive_Syndication_Q_A__c,
		Receive_Syndication_Reviews__c,
		Send_Syndication_Curations__c,
		Send_Syndication_Q_A__c,
		Send_Syndication_Reviews__c,
		Platform__c,
		Account__r.Name,
		Account__r.Type,
		Account__r.Industry,
		Account__r.Netsuite_Internal_ID__c,
		Account__r.Vertical__c,
		Account__r.CS_Vertical__c, 
		convertCurrency(Account__r.Booked_ASF_Netsuite2__c), 
		Account__r.Netsuite_Account_Name__c, 
		Account__r.BillingCountry,
		Account__r.ShippingCountry, 
		Account__r.RAD_Score__c,
		Account__r.CS_Segment__c,
		Account__r.Region__c, 
		Account__r.Sub_Region__c,
		Account__r.Country_Code__c, 
		Account__r.Company_Size__c, 
		Account__r.Website, 
		Account__r.EngagementTier__c,
		Account__r.Account_Classification__c,
		Account__r.Account_Tier__c,
		Account__r.Parent.Name,
		Account__r.Owner.Name,
		Account__r.Client_Success_Director2__r.Name,
		Account__r.Client_Partner2__r.Name
	FROM BVSite__c
"""

ma_clients = """
	SELECT '{0}' as Cluster, ID as ClientID, LOWER(Name) as Name
	FROM Client
	WHERE Enabled = 1
	AND SyndicationParentClientID IS NULL
"""

truncate = """
	TRUNCATE TABLE Clients
"""

insert = """
INSERT INTO Clients
	(`id`, `name`, `cluster`, `billingid`, `sf_accountname`, 
	`parent_account`, `AccountType`, `sf_cs_vertical`, `sf_industry`, 
	`sf_sub_vertical`, `sf_csd`, `account_owner`, `BookedASF`, 
	`netsuite_accountname`, `sf_id`, `sf_billingcountry`, 
	`sf_shippingcountry`, `sf_rad_score`, `sf_csp`, `OPT_IN`, 
	`CS_Segment`, `Region`, `Sub_Region`, `Country_Code`, `Company_Size`,
	`Account_Classification`, `Website`, `Engagement_Tier`, `Retail_Priority`,
	`BV_Platform`, `Receive_Brand_Answers`, `Receive_Brand_Rev_Responses`,
	`Receive_Syndication_Curations`, `Receive_Syndication_Q_A`, 
	`Receive_Syndication_Reviews`, `Send_Syndication_Curations`, 
	`Send_Syndication_Q_A`, `Send_Syndication_Reviews`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
	%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
	%s, %s, %s, %s, %s, %s, %s)
"""
