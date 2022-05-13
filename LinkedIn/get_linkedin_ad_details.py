import requests
import json


def get_linkedin_ad_details_creatives(ad_id, access_token):
    """ A function which access LinkedIn Ads - Sponsored Update Creative and gives us their title and name """

    # Define headers
    headers = {'Authorization': 'Bearer ' + access_token}

    # Define URL API
    url = 'https://api.linkedin.com/v2/adCreativesV2/' + str(ad_id)
    url += '?projection=(variables(data(*,com.linkedin.ads.SponsoredUpdateCreativeVariables'
    url += '(*,share~(subject,text(text), content(contentEntities(*(description,entityLocation,title))))))))'

    # Make the http call
    r = requests.get(url=url, headers=headers)
    ads_response_dict = json.loads(r.text)

    # Access information about Sponsored Update Creative Ads
    if 'com.linkedin.ads.SponsoredUpdateCreativeVariables' in ads_response_dict['variables']['data']:

        ads_variables = ads_response_dict['variables']['data']['com.linkedin.ads.SponsoredUpdateCreativeVariables']['share~']

        ad_title = ads_variables['content']['contentEntities'][0]['title']
        ad_name = ads_variables['subject']

    else:
        ad_title = None
        ad_name = None

    return ad_title, ad_name


def get_linkedin_ad_details(ad_id, ad_reference, access_token):
    """ A function which access LinkedIn Ads and gives us their title and name """

    # Define headers
    headers = {'Authorization': 'Bearer ' + access_token}

    # Define URL API
    url = 'https://api.linkedin.com/v2/adDirectSponsoredContents/'
    url += 'urn:li:ugcPost:' + ad_reference

    # Make the http call
    r = requests.get(url=url, headers=headers)
    ads_response_dict = json.loads(r.text)

    ad_title = None

    if 'name' in ads_response_dict.keys():
        ad_name = ads_response_dict['name']
    else:
        ad_title, ad_name = get_linkedin_ad_details_creatives(ad_id, access_token)

    return ad_title, ad_name
