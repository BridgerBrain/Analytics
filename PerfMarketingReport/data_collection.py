

def data_collection():
    project_id = "gtm-wtb9mc4-m2m0z"

    data_query = """SELECT
    
    CAST(Leads.id AS int) as Lead_ID, 
    Leads.Created_Time,
    Leads.Lead_Status, 
    Leads.Demo_Date as Demo_Date,
    Leads.Is_Test as Is_Test,
    Leads.Demo_Status as Demo_Status,
    Leads.Calc_converted_detail.account as Converted_Account_ID, 
    Leads.Calc_converted_detail.deal as Converted_Deal_ID, 
    
    Accounts.First_Trx as Accounts_Adjusted_First_Trx,
    CAST(Accounts.id AS int) as Accounts_Adjusted_Account_ID,
    Accounts.Created_Time as Accounts_Adjusted_Created_Time,
    Accounts.Lead_Status as Accounts_Adjusted_Lead_Status,
    Accounts.Subscription_Plan as Accounts_Adjusted_Subscription_Plan,
    Accounts.Account_Name as Accounts_Adjusted_Account_Name,
    Accounts.Demo_Date as Accounts_Adjusted_Demo_Date,
    Accounts.Lead_Created_Time as Accounts_Adjusted_Lead_Created_Time,
    Accounts.Activation_Key_Activated as Accounts_Adjusted_Activation_Key_Activated, 
    Accounts.PSP_MID_Connected as Accounts_Adjusted_PSP_MID_Connected, 
    Accounts.Is_Test as Accounts_Adjusted_Is_Test,
    Accounts.Demo_Status as Accounts_Adjusted_Demo_Status,
    
    CAST(Deals.id AS int) as Deals_Adjusted_Deal_ID, 
    Deals.Deal_Name as Deals_Adjusted_Deal_Name,
    Deals.Account_Name.id as Deals_Adjusted_Account_ID, 
    Deals.Stage as Deals_Adjusted_Stage, 
    Deals.Subscription_Plan as Deals_Adjusted_Subscription_Plan, 
    Deals.Closing_Date as Deals_Adjusted_Closing_Date, 
    Deals.Lead_Created_Time as Deals_Adjusted_Lead_Created_Time,
    Deals.Is_Test as Deals_Adjusted_Is_Test, 
    Deals.Expected_MRR as Deals_Adjusted_Expected_MRR, 
    Deals.Amount as Deals_Adjusted_Amount, 
    Deals.Currency as Deals_Adjusted_Currency,
    
    Leads.Campaign_ID1, 
    Accounts.Campaign_ID, 
    Leads.Campaign_UTM1,
    
    Accounts.Campaign_Name, 
    Leads.Source_UTM1, 
    Accounts.Campaign_Source,
    
    Leads.Refresh_Time as Leads_Refresh_Time,
    Accounts.Refresh_Time as Accounts_Adjusted_Refresh_Time,
    Deals.Refresh_Time as Deals_Adjusted_Refresh_Time
    
    FROM `gtm-wtb9mc4-m2m0z.zoho_dataset.crm_Leads` Leads
    FULL OUTER JOIN `gtm-wtb9mc4-m2m0z.zoho_dataset.crm_Accounts` Accounts
    ON Leads.Calc_converted_detail.account = Accounts.id
    
    FULL OUTER JOIN `gtm-wtb9mc4-m2m0z.zoho_dataset.crm_Deals` Deals
    ON Accounts.id = Deals.Account_Name.id
    
    WHERE
    Leads.Is_Test IS NOT TRUE
    AND Accounts.Is_Test IS NOT TRUE
    AND Deals.Is_Test IS NOT TRUE
    
    """

    return project_id, data_query
