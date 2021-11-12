# smxl_it_2021#

## Setup

1. Enable Google Transfer Service for Google Ads
  - set up backfills for your preferred date range ( each may span up to 6 months, so you may consider to request 2 backfills to get data for one year )
  - wait for the Transfer Service to download everything ( It may take several weeks to get data for 1 year )
2. Enable Transfer-Service-Augmenter-Script
  - In Google Ads UI go to your MCC account and create a new Script
  - paste the Augmenter script source code
  - enable advanced API: BigQuery
  - fill out the first 3 lines of the script
    - the project id of your Bigquery-project
    - the dataset-id which was used to create the transfer-service
    - optionally an email adress to receive messages if something goes wrong.
  - schedule it to run hourly or daily
3. Create a new App Script in https://script.google.com/home
  - copy the script "smxl_it_2021.js" into the UI and save it.
  - enable services:
    - Bigquery
    - Google Analytics
  - Fill out mandatory settings
    - your Google Analytics Views-Ids ( should correspond (and be linked ) to the Google Ads accounts )
    - your Google Cloud projectId
    - the name of the dataset used for Transfer-Service
    - Bigquery Location ( EU or US )
    - Google Ads AccountId used for Transfer-Service
  - adjust optional settings
    - optionally set a prefix for pitfall views in Bigquery
    - optionally change the names of Datasets
    - LIMIT_ANALYTICS_DATA_PER_EXECUTION_IN_DAYS - amount of days of data which is downloaded from Analytics at every execution. Default is 1 day.
    - HOUR_INTERVAL - the script will be executed every x hours. Default is 1 hour.
    - MAX_DAYS_OF_ANALYTICS_DATA ( default is last 365 days ) - Script will download some days worth of data at each execution until it reaches this limit.
    - MAX_DAYS_OF_GOOGLE_ADS_DATA ( default is last 365 days ) - Views will limit the data to this date range. Larger date ranges will increase query costs.
4. Execute the script by clicking on "run" ( "main" function should be selected )
  - Script will register a trigger automatically and will be executed every HOUR_INTERVAL hours (default is 1 = hourly).
  - at each execution it will re-compute all views and write the results into tables ( which in turn can be linked to Data Studio Dashboard )
  - at each execution it will get LIMIT_ANALYTICS_DATA_PER_EXECUTION_IN_DAYS days (default is 1 day) of Google Analytics data
    - you can increase this setting but keep in mind that the script has only 30 minutes for each execution and getting data from Google Analytics takes some time
5. Link a copy of the Datastudio to BigQuery data sources
  - The script from step 4 + 5 will generate tables with "1_" prefix. Connect these datasources to the Datastudio dashboards.
  - Common-Pitfalls-DS: https://datastudio.google.com/u/0/reporting/1e7c667b-01a9-4253-a0c9-55d353bd2e66/page/eZXR (You will be granted VIEW access - make a copy of the GDS dashboard first and connect the datasources to this dashboard)
  - Stay-In-Control-DS: https://datastudio.google.com/u/0/reporting/0dc256f8-da02-4468-9d74-322cf3317496/page/IUyMC (You will be granted VIEW access - make a copy of the GDS dashboard first and connect the datasources to this dashboard)

6. Repair your copy of the dashboard ( DataStudio doesn't copy everything correctly )
  - computed fields are dropped in the copy of the dashboard, so we need to re-create them
    - the data source "missing" needs 4 additional computed fields:
    - MissingClicksRate : SUM( MissingClicks ) / SUM( keywordClicks )
    - MissingConversionRate : SUM( MissingConversions ) / SUM( keywordConversions )
    - MissingCostRate : SUM( MissingCost ) / SUM( keywordCost )
    - MissingImpressionRate : SUM( MissingImpressions ) / SUM( keywordImpressions )
    - These 4 fiels are used in "Before 01/09/2020" and "After 01/09/2020" charts on page2 "Impact of misisng search terms"
  - adjust names and types of fields according to template dashboard

## Setup Cookie Consent Checker

1. Navigate to Cloud Functions in the Google Cloud Project and create a new function.
  -  Give the function a name and select an EU region. The trigger must be set to "Allow unauthenticated invocations". Click on Save and then Next.  
  -  Select "Python 3.7" under Runtime. Copy the contents of main.py into the function main.py. Do the same with the Requirement.txt.
  -  Adjust your table id in the main.py.
  -  Change the entry point to "cookie_consent_check".
  -  Deploy the function.

2. Navigate to the Custom Templates in the Google Tag Manager and create a new one there. 
  - Click on the menu in the upper right corner and then on import. Select the "cookie-consent-checker.tpl" file and import it.
  - Create a new tag under Tags and select the Cookie Consent Checker.
  - The cloud function URL is located at the previously created function under Trigger. Copy it once and paste it into the field. 
  - Tragen sie den Namen des Cookie Consent Cookies ein und hinterlegen sie die "Click Text" Variable. All other settings are optional.
  - Now create a Visibility Trigger for the impression and a Click Trigger for clicking on the desired buttons of the Consent Banner. 
  - Test once in preview mode if the tag fires and then submit it.  

3. There should now be a table in the Big Query called "cookie_consent_request_table". Link this table with the "Stay-In-Control-DS" Datastudio and create the following custom fields in the data source.

Banner Accepted (For the version where you have enabled the Consent Status in the tag). 
if(CONTAINS_TEXT(Consent_Status, 'Accept all') = True,1,0) 
Here, instead of 'Accept all', enter what you have defined as "Accept banner" in the tag.  

Banner Accepted (For the version where you have NOT activated the Consent Status in the tag) 
if(CONTAINS_TEXT(Click_Text, 'Accept all') = True,1,0) 
Here, instead of 'Accept all', you enter what will be the text in the button when your banner is accepted.  

Banner not Accepted (For the version where you have activated the Consent status in the tag) 
if(CONTAINS_TEXT(Consent_Status, 'Accepted') = True,0,1) 
Here, instead of 'Accept all', enter what you have defined as "Accept banner" in the tag.  

Banner not Accepted (For the version where you have NOT activated Consent Status in the tag) 
if(CONTAINS_TEXT(Click_Text, 'Accept all') = True,1,0) 
Here, instead of 'Accept all', you enter what will be the text in the button when your banner is accepted.  

Accepted Rate
sum(Banner Accepted) / sum(bannerImpression) 
Here you can set the field in advance, because it is a percentage.  
 
Banner CTR 
sum(bannerClick) / sum(bannerImpression) 
Here you can set the field in advance, since it is a percentage.  

(Coming soon)
A detailed tutorial or single implementation of the Cookie Consent Checker can be found here:
https://github.com/peakace/cookie-consent-checker 

