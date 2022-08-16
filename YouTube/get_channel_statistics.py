import pandas as pd
from datetime import datetime


def get_channel_statistics(yt_api, yt_channel_id):
    """ A function which uses a channel id and returns us information about the channel. We get channel metrics
        like total views, subscribers and videos, information about the playlists and the channel creation date."""

    channel_statistics = []

    # Connect to the YouTube channel
    channel_metadata = yt_api.get_channel_metadata(yt_channel_id)

    # Gather channel data
    channel_id = channel_metadata['channel_id']
    channel_title = channel_metadata['title']
    channel_url = f'https://www.youtube.com/channel/{channel_id}'
    playlist_ids = [channel_metadata['playlist_id_uploads']]
    total_viewers = channel_metadata['view_count']
    total_subscribers = channel_metadata['subscription_count']
    total_videos = channel_metadata['video_count']

    # Converting a UTC timestamp for account creation date to DateTime format
    account_creation_date = datetime.utcfromtimestamp(channel_metadata['account_creation_date']).strftime('%Y-%m-%d %H:%M:%S')

    # Save channel details to a list, then to a dataframe and save the table as a .csv file
    channel_statistics.append([channel_title, channel_id, channel_url,
                               playlist_ids,
                               total_viewers, total_subscribers, total_videos,
                               account_creation_date])

    channel_statistics = pd.DataFrame(channel_statistics, columns=['channel_name', 'channel_id', 'channel_URL',
                                                                   'playlists',
                                                                   'total_views', 'total_subscribers', 'total_videos',
                                                                   'account_creation_date'])

    channel_statistics.to_csv('datasets/channel_statistics.csv', index=False)
    channel_statistics.to_csv('../../Desktop/Projects/YouTube_API/datasets/channel_statistics.csv', index=False)

    return channel_statistics
