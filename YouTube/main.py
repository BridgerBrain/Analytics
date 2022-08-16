from youtube_api import YouTubeDataAPI
from datetime import datetime

from get_youtube_variables import get_youtube_variables
from get_channel_statistics import get_channel_statistics
from get_channel_video_statistics import get_channel_video_statistics
from get_playlist_statistics import get_playlist_statistics

# Set starting running date
starting_timestamp = datetime.now()
# print('STARTING DATETIME: ', starting_timestamp, '\n')

# Get YouTube credentials
my_youtube_api_username, my_youtube_api_key, my_youtube_playlist_id, my_youtube_channel_id = get_youtube_variables()

# Access YouTube API using the API key
my_youtube_api = YouTubeDataAPI(my_youtube_api_key)

# Get BridgerPay channel statistics
bridgerpay_channel_stats = get_channel_statistics(my_youtube_api, my_youtube_channel_id)
print("BridgerPay Channel Statistics Dataset Shape:", bridgerpay_channel_stats.shape, '\n')

# Get statistics for videos uploaded on BridgerPay channel
bridgerpay_channel_videos_stats = get_channel_video_statistics(my_youtube_api, my_youtube_channel_id, 'BridgerPay')
print("BridgerPay Channel Video Statistics Dataset Shape:", bridgerpay_channel_videos_stats.shape, '\n')

# Get statistics and duration of a BridgerPay's playlist using its ID
# print_info = False
print_info = True
bridgerpay_playlist_stats = get_playlist_statistics(my_youtube_api_key, my_youtube_playlist_id, print_info)

# Set ending running date
ending_timestamp = datetime.now()
# print('ENDING DATETIME: ', ending_timestamp, '\n')

# Calculate time needed to run and collect data
total_time_needed = ending_timestamp - starting_timestamp
print('TIME NEEDED: ', total_time_needed)

