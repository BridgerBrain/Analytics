import os


def get_environmental_variable(variable):
    """ A function which helps us to get environmental variables and raises an error if a key do not exist """

    env_variable = os.getenv(variable, None)

    if env_variable is None:
        print("ERROR: MISSING {}".format(variable))

        exit(1)

    return env_variable


def get_youtube_variables():
    """ A function which gives us the environmental variables, such as API username and key.
        It also gives us channel id and playlist id. """

    # Accessing environmental variables using the relevant function
    yt_api_username = get_environmental_variable('MY_YOUTUBE_API_USERNAME3')
    yt_api_key = get_environmental_variable('MY_YOUTUBE_API_KEY3')
    yt_playlist_id = get_environmental_variable('MY_YOUTUBE_PLAYLIST_ID3')
    yt_channel_id = get_environmental_variable('MY_YOUTUBE_CHANNEL_ID3')

    # An option to print the environmental variables
    print_info = False
    if print_info:
        print("=================== YOUTUBE API CREDENTIALS ===================")
        print('YouTube API USERNAME: ', yt_api_username)
        print('YouTube API KEY:      ', yt_api_key)
        print('YouTube PLAYLIST ID:  ', yt_playlist_id)
        print('YouTube CHANNEL ID:   ', yt_channel_id)
        print("=================== YOUTUBE API CREDENTIALS ===================")

    return yt_api_username, yt_api_key, yt_playlist_id, yt_channel_id
