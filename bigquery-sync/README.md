# bi Production

# Main Code to run on designated schedule

## Code: run_main 
Description: This will retrieve the following modules from Zoho and upload them to the zoho_dataset in BigQuery analytics-bridgerpay-prod. 
Generated tables Example: crm_Leads, books_invoices in gtm-wtb9mc4-m2m0z - zoho_dataset

### Uploaded Modules:
 * Zoho CRM 
    * Leads
    * Deals
    * Contacts
    * Accounts
    * PSPs
* Zoho Books
  * invoices
  * creditnotes
  * contacts
  * customerpayments
   
## Schedule
Run between 07:45 to 19:45 daily every hour 

## Dependencies
* Get_Zoho_CRM_Modules
* Get_Zoho_Books_Modules
* Upload_Zoho_to_Big_Query

## Python Libraries
* json
* os
* pandas
* requests
* datetime
* google-api-core
* google-auth
* google-auth-oauthlib
* google-cloud-bigquery
* google-cloud-core

## Environmental Variables
* ZOHO_CRM_REFRESH_TOKEN
* ZOHO_CRM_CLIENT_ID
* ZOHO_CRM_CLIENT_SECRET
* GOOGLE_APPLICATION_CREDENTIALS (path to JSON file)
* ZOHO_BOOKS_REFRESH_TOKEN
* ZOHO_BOOKS_CLIENT_ID
* ZOHO_BOOKS_CLIENT_SECRET
* ZOHO_BOOKS_ORGANIZATION_ID

## Code: get_AdWords_Info
Description: Uses the Google AdWords API to retrieve all the GCLIDs and basic campaign information of the previous date. If any GCLID matches
with a Lead stored in BigQuery table crm_Leads it will upload the campaign information to the BigQuery table: google_adwords_info

## Schedule
daily at 08:00 am

## Code: get_AdWords_Metrics
Description: Uses the Google AdWords API to retrieve google campaign metrics of the previous date. it will upload the metrics to the BigQuery table: google_Metrics

## Schedule
daily at 08:00 am

## Python Libraries
* json
* os
* pandas
* requests
* datetime
* time
* google-ads
* google-api-core
* google-auth
* google-auth-oauthlib
* google-cloud-bigquery
* google-cloud-core

## Environmental Variables
* DEVELOPER_TOKEN
* GOOGLE_APPLICATION_CREDENTIALS (path to JSON file)
* GOOGLE_CUSTOMER_ID
* SUBJECT_EMAIL

## Code: get_facebook_Data
Description: Uses the Facebook API to retrieve Ad Insights with breakdown by Country of the previous day. 
It will then upload this to BigQuery to table fb_marketing.

## Environmental Variables
* FB_ACCESS_TOKEN
* GOOGLE_APPLICATION_CREDENTIALS (path to JSON file)
* FB_APP_SECRET
* FB_APP_ID

## Schedule
daily at 08:00 am
