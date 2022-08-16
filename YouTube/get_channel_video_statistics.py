import pandas as pd
from datetime import datetime


def get_channel_video_statistics(yt_api, yt_channel_id, channel_name='BridgerPay'):
    """ A function that searches a YouTube channel name and by connecting to the YouTube API it gives us information
        about the videos of the channel. We get data like channel title, channel id, video url, video id,
        video title, video description, video publish date and video thumbnail.  """

    channel_videos = []

    # Search all the videos that are available on YouTube containing the given keyword
    yt_search = yt_api.search(channel_name, max_results=1000)

    for vid in yt_search:

        # Make sure that we gather information only for videos uploaded from the channel that we want
        if vid['channel_id'] == yt_channel_id:

            # Converting a UTC timestamp for video publish date to DateTime format
            vid_publish_date = datetime.utcfromtimestamp(vid['video_publish_date']).strftime('%Y-%m-%d %H:%M:%S')

            # Create a video URL using the video id
            vid_id = vid['video_id']
            vid_url = f'https://youtu.be/{vid_id}'

            vid_id_metadata = yt_api.get_video_metadata(vid_id)

            vid_category = vid_id_metadata['video_category']
            vid_views = vid_id_metadata['video_view_count']
            vid_comments = vid_id_metadata['video_comment_count']
            vid_likes = vid_id_metadata['video_like_count']
            vid_dislikes = vid_id_metadata['video_dislike_count']
            if type(vid_dislikes) != int:
                vid_dislikes = 0

            # Save video details to a list, then to a dataframe and save the table as a .csv file
            channel_videos.append([vid['channel_title'], vid['channel_id'], vid_url,
                                   vid_id, vid['video_title'], vid['video_description'],
                                   vid_publish_date, vid['video_thumbnail'], vid_category,
                                   vid_views, vid_likes, vid_dislikes,
                                   vid_comments])

    channel_videos = pd.DataFrame(channel_videos, columns=['channel_title', 'channel_id', 'video_URL',
                                                           'video_id', 'video_title', 'video_description',
                                                           'video_publish_date', 'video_thumbnail', 'video_category',
                                                           'video_views', 'video_likes', 'video_dislikes',
                                                           'video_comments'])

    channel_videos.to_csv('datasets/bridgerpay_videos.csv', index=False)
    channel_videos.to_csv('../../Desktop/Projects/YouTube_API/datasets/bridgerpay_videos.csv', index=False)

    return channel_videos
