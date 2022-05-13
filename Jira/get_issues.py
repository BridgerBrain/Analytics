import pandas as pd
import datetime

from check_working_datetime import check_working_datetime


def get_issues(my_jira, max_res):
    """ A function which access Jira issues and gives us all the required information about them """

    proj_issues_df = pd.DataFrame([])

    # Set project name
    proj_names = ['Support Service Desk']

    if 'historical' in max_res:
        max_results = None
    elif 'last' in max_res:
        max_results = int(max_res[-3:])
    else:
        max_results = 1

    for _ in proj_names:

        proj_issues = []

        print("\t Now accessing projects...")

        # Search for all the issues which are raised in the project
        issues_in_proj = my_jira.search_issues('project= "Support Service Desk"', startAt=0, maxResults=max_results)

        print("\t \t Support Service Desk Project now accessed.")
        print("\t \t \t Now accessing issues...")

        # Access information for each issue in the project
        for singleIssue in issues_in_proj:

            print("\t Now processing issue ", singleIssue)

            # Get all the available information for a specific issue
            issue = my_jira.issue(singleIssue)
            issue_fields = issue.raw['fields']

            temp_key = singleIssue

            if issue_fields['created'] is not None:
                temp_created = issue_fields['created']
                temp_created = temp_created[:10] + ' ' + temp_created[11:19]
                temp_created = datetime.datetime.strptime(temp_created, '%Y-%m-%d %H:%M:%S')
            else:
                break

            if issue_fields['resolutiondate'] is not None:
                temp_resolution_date = issue_fields['resolutiondate']
                temp_resolution_date = temp_resolution_date[:10] + ' ' + temp_resolution_date[11:19]
                temp_resolution_date = datetime.datetime.strptime(temp_resolution_date, '%Y-%m-%d %H:%M:%S')
            else:
                temp_resolution_date = None

            if issue_fields['resolution'] is not None:
                temp_resolution_name = issue_fields['resolution']['name']
            else:
                temp_resolution_name = 'Unresolved'

            temp_status = issue_fields['status']['name']

            if issue_fields['customfield_10037'] is not None:
                temp_merchant_name = issue_fields['customfield_10037']
            else:
                temp_merchant_name = None

            temp_summary = issue_fields['summary']

            if issue_fields['issuelinks'] is not None and type(issue_fields['issuelinks']) is not list:
                temp_issue_links = issue_fields['issuelinks']['inwardIssue']['key']
            else:
                temp_issue_links = None

            if issue_fields['customfield_10048'] is not None:
                temp_merchant_type = issue_fields['customfield_10048'][0]['value']
            else:
                temp_merchant_type = None

            if issue_fields['customfield_10010'] is not None:
                if len(issue_fields['customfield_10010']) > 0:
                    if 'requestType' in issue_fields['customfield_10010'].keys():
                        temp_request_type = issue_fields['customfield_10010']['requestType']['name']
                    else:
                        temp_request_type = None
                else:
                    temp_request_type = None
            else:
                temp_request_type = None

            temp_issue_type = issue_fields['issuetype']['name']

            if issue_fields['components'] is not None:
                if len(issue_fields['components']) > 0:
                    temp_components = issue_fields['components'][0]['name']
                else:
                    temp_components = None
            else:
                temp_components = None

            if issue_fields['customfield_10057'] is not None:
                temp_tier = issue_fields['customfield_10057']['value']
            else:
                temp_tier = None

            if issue_fields['priority'] is not None:
                temp_priority = issue_fields['priority']['name']
            else:
                temp_priority = None

            if issue_fields['customfield_10004'] is not None:
                temp_impact = issue_fields['customfield_10004']['value']
            else:
                temp_impact = None

            if issue_fields['reporter'] is not None:
                temp_reporter = issue_fields['reporter']['displayName']
            else:
                temp_reporter = None

            if issue_fields['assignee'] is not None:
                temp_assignee = issue_fields['assignee']['displayName']
            else:
                temp_assignee = 'Unassigned'

            if issue_fields['updated'] is not None:
                temp_updated = issue_fields['updated']
                temp_updated = temp_updated[:10] + ' ' + temp_updated[11:19]
                temp_updated = datetime.datetime.strptime(temp_updated, '%Y-%m-%d %H:%M:%S')
            else:
                temp_updated = None

            if issue_fields['duedate'] is not None:
                temp_due = issue_fields['duedate']
            else:
                temp_due = None

            if issue_fields['customfield_10030'] is not None and issue_fields['customfield_10030']['name'] == 'Time to first response':
                if len(issue_fields['customfield_10030']['completedCycles']) > 0:

                    temp_start_response_time = issue_fields['customfield_10030']['completedCycles'][0]['startTime'][
                        'iso8601']
                    temp_start_response_time = temp_start_response_time[:10] + ' ' + temp_start_response_time[11:19]
                    temp_start_response_time = datetime.datetime.strptime(temp_start_response_time, '%Y-%m-%d %H:%M:%S')

                    temp_end_response_time = issue_fields['customfield_10030']['completedCycles'][0]['stopTime'][
                        'iso8601']
                    temp_end_response_time = temp_end_response_time[:10] + ' ' + temp_end_response_time[11:19]
                    temp_end_response_time = datetime.datetime.strptime(temp_end_response_time, '%Y-%m-%d %H:%M:%S')

                else:
                    temp_start_response_time = None
                    temp_end_response_time = None

            else:
                temp_start_response_time = None
                temp_end_response_time = None

            # Access datetime details and check if they are "working" datetime
            temp_day_created = temp_created.strftime("%A")
            temp_date_created = datetime.datetime.date(temp_created)
            temp_time_created = datetime.datetime.time(temp_created)
            check_datetime_created = check_working_datetime(temp_created, temp_date_created,
                                                            temp_time_created, temp_day_created)

            if temp_end_response_time is not None:
                temp_day_response = temp_end_response_time.strftime("%A")
                temp_date_response = datetime.datetime.date(temp_end_response_time)
                temp_time_response = datetime.datetime.time(temp_end_response_time)
                check_datetime_responded = check_working_datetime(temp_end_response_time, temp_date_response,
                                                                  temp_time_response, temp_day_response)
            else:
                check_datetime_responded = None

            if issue_fields['customfield_10027'] is not None:
                temp_rating = issue_fields['customfield_10027']['rating']
            else:
                temp_rating = None

            # Save issues information to a list, then to a dataframe and save the table as a .csv file
            proj_issues.append([temp_key, temp_status, temp_merchant_name, temp_summary,
                                temp_issue_links, temp_merchant_type, temp_request_type, temp_issue_type,
                                temp_components, temp_tier, temp_impact, temp_reporter,
                                temp_assignee, temp_priority,
                                temp_created, temp_updated, temp_due,
                                temp_resolution_name, temp_resolution_date,
                                temp_created,
                                temp_start_response_time, temp_end_response_time,
                                check_datetime_created, check_datetime_responded,
                                temp_rating])

            proj_issues_df = pd.DataFrame(proj_issues,
                                          columns=['key', 'status', 'merchant_name', 'summary',
                                                   'links', 'merchant_type', 'request_type', 'issue_type',
                                                   'components', 'tier', 'impact', 'reporter',
                                                   'assignee', 'priority',
                                                   'created_datetime', 'updated_datetime', 'due_datetime',
                                                   'resolution', 'resolution_datetime',
                                                   'created_full_date',
                                                   'start_response_full_date', 'end_response_full_date',
                                                   'next_working_created', 'next_working_responded',
                                                   'customer satisfaction rating'])

    return proj_issues_df
