import re
from datetime import timedelta


def transform_sec_to_duration(temp_seconds):
    """ A function which is used to transform seconds to video duration as hours, minutes and seconds """

    temp_seconds = int(temp_seconds)
    temp_minutes, temp_seconds = divmod(temp_seconds, 60)
    temp_hours, temp_minutes = divmod(temp_minutes, 60)

    return temp_hours, temp_minutes, temp_seconds


def get_playlist_duration(vid_duration_response, my_youtube_playlist_id):
    """ A function which is used to get the total duration of a playlist by inserting the playlist ID """

    # Initiating parameters
    hours_pattern = re.compile(r'(\d+)H')
    minutes_pattern = re.compile(r'(\d+)M')
    seconds_pattern = re.compile(r'(\d+)S')

    total_seconds = 0
    video_duration_details = []

    # Access information about the duration of each video in hours, minutes and seconds
    for item in vid_duration_response['items']:

        duration = item['contentDetails']['duration']

        hours = hours_pattern.search(duration)
        minutes = minutes_pattern.search(duration)
        seconds = seconds_pattern.search(duration)

        hours = int(hours.group(1)) if hours else 0
        minutes = int(minutes.group(1)) if minutes else 0
        seconds = int(seconds.group(1)) if seconds else 0

        # Calculate the duration of a video in seconds
        video_seconds = int(timedelta(hours=hours,
                                      minutes=minutes,
                                      seconds=seconds).total_seconds())

        # Transform seconds to hours, minutes and seconds using this function for each video in the playlist
        item_hours, item_minutes, item_seconds = transform_sec_to_duration(video_seconds)

        # Create YouTube URL
        video_id = item['id']
        video_url = f'https://youtu.be/{video_id}'

        # Append video duration details and IDs to a list
        video_duration_details.append([my_youtube_playlist_id, video_url, video_id,
                                       item_hours, item_minutes, item_seconds])

        # Calculate the total duration of the playlist
        total_seconds += video_seconds

    return video_duration_details, total_seconds


def get_playlist_stats(vid_stats_response, my_youtube_playlist_id):
    """ A function which is used to get the metrics of a playlist by giving the playlist ID """

    # Initiating parameters
    total_views = 0
    total_likes = 0
    total_comments = 0
    video_stats_details = []

    # Access statistics for each video in hours, minutes and seconds
    for item in vid_stats_response['items']:

        # Create YouTube URL
        video_id = item['id']
        video_url = f'https://youtu.be/{video_id}'

        # Get information about views, likes and comments
        video_views = int(item['statistics']['viewCount'])
        video_likes = int(item['statistics']['likeCount'])
        video_comments = int(item['statistics']['commentCount'])

        # Append video statistics and IDs to a list
        video_stats_details.append([my_youtube_playlist_id, video_url, video_id,
                                    video_views, video_likes, video_comments])

        # Calculate the total number of views, likes and comments of the playlist
        total_views += video_views
        total_likes += video_likes
        total_comments += video_comments

    return video_stats_details, total_views, total_likes, total_comments
