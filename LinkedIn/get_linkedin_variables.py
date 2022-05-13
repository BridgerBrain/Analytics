import os


def get_environmental_variable(variable):
    """ A function which helps us to get environmental variables and raises an error if a key do not exist """

    env_variable = os.getenv(variable, None)

    if env_variable is None:
        print("ERROR: MISSING {}".format(variable))

        exit(1)

    return env_variable


def get_linkedin_variables():
    """ A function which gives us the environmental variables, such as API username and access key.
        It could also give us channel id and playlist id. """

    # Accessing environmental variables using the relevant function
    linkedin_access_token = get_environmental_variable('LINKEDIN_ACCESS_TOKEN1')
    linkedin_profile_id = get_environmental_variable('LINKEDIN_PROFILE_ID')

    # An option to print the environmental variables
    # print_info = True
    print_info = False
    if print_info:
        print("=================== LINKEDIN API CREDENTIALS ===================")
        print('LINKEDIN ACCESS TOKEN: ', linkedin_access_token)
        print('LINKEDIN PROFILE ID:   ', linkedin_profile_id)
        print("=================== LINKEDIN API CREDENTIALS ===================")
        print()

    return linkedin_access_token, linkedin_profile_id
