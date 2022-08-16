from googleapiclient.discovery import build
import pandas as pd

from get_playlist_aux_functions import transform_sec_to_duration
from get_playlist_aux_functions import get_playlist_duration
from get_playlist_aux_functions import get_playlist_stats


def get_playlist_statistics(my_youtube_api_key, my_youtube_playlist_id, print_info=True):
    """ A function which uses a playlist ID and gives us information and metrics about YouTube
        videos in the playlist """

    nextpagetoken = None

    # Access YouTube API
    youtube_api = build('youtube', 'v3', developerKey=my_youtube_api_key)

    while True:

        # A request to access details of a specific playlist in a YouTube channel
        pl_request = youtube_api.playlistItems().list(part='contentDetails',
                                                      playlistId=my_youtube_playlist_id,
                                                      maxResults=50,
                                                      pageToken=nextpagetoken)

        # Executing the elements of the playlist
        pl_response = pl_request.execute()
        print(pl_response)

        # Access VideoIDs of videos in the playlist
        vid_ids = []
        for item in pl_response['items']:
            vid_ids.append(item['contentDetails']['videoId'])

        # A request to access duration details of each video in the playlist
        vid_duration_request = youtube_api.videos().list(part="contentDetails",
                                                         id=','.join(vid_ids))

        # Executing duration details of videos in the playlist
        vid_duration_response = vid_duration_request.execute()

        # A request to access statistics for each video in the playlist
        vid_stats_request = youtube_api.videos().list(part="statistics",
                                                      id=','.join(vid_ids))
        # Executing statistics of videos in the playlist
        vid_stats_response = vid_stats_request.execute()

        # Get duration and statistics information from videos in the playlist in two lists and summative information
        video_duration_details, total_seconds = get_playlist_duration(vid_duration_response, my_youtube_playlist_id)
        video_stats_details, total_views, total_likes, total_comments = get_playlist_stats(vid_stats_response,
                                                                                           my_youtube_playlist_id)

        nextpagetoken = pl_response.get('nextPageToken')
        if not nextpagetoken:
            break

    # Add summative information about duration and statistics in the lists
    total_hours, total_minutes, total_seconds = transform_sec_to_duration(total_seconds)

    video_duration_details.append([my_youtube_playlist_id, 'None', 'TotalDuration',
                                   total_hours, total_minutes, total_seconds])

    video_stats_details.append([my_youtube_playlist_id, 'None', 'TotalDuration',
                                total_views, total_likes, total_comments])

    # Create a dataframe containing duration details of videos in the playlist
    playlist_duration_df = pd.DataFrame(video_duration_details,
                                        columns=['playlist_id', 'video_URL', 'video_id',
                                                 'video_hours', 'video_minutes', 'video_seconds'])

    # Create a dataframe containing statistics of videos in the playlist
    playlist_stats_df = pd.DataFrame(video_stats_details,
                                     columns=['playlist_id', 'video_URL', 'video_id',
                                              'video_views', 'video_likes', 'video_comments'])

    # Create a dataframe which concatenates both duration and statistics of videos in the playlist
    playlist_df = pd.concat([playlist_stats_df,
                             playlist_duration_df[['video_hours', 'video_minutes', 'video_seconds']]], axis=1)

    # Save dataframe as a .csv file
    playlist_df.to_csv('datasets/playlist_information.csv', index=False)
    playlist_df.to_csv('../../Desktop/Projects/YouTube_API/datasets/playlist_information.csv', index=False)

    # Print information about the Playlist
    if print_info:
        print("=============================== PLAYLIST INFORMATION ===============================")
        print(f'Total Duration of Playlist with ID: \t\t {my_youtube_playlist_id}'
              f'\n \t\t\t\t\t\t {total_hours} hours : {total_minutes} minutes : {total_seconds} seconds')
        print()
        print(f'Summative Statistics of Playlist with ID: \t {my_youtube_playlist_id}'
                                              f' \n \t\t\t\t\t\t\t Total views: \t {total_views:,} '
                                              f' \n \t\t\t\t\t\t\t Total likes: \t {total_likes:,} '
                                              f' \n \t\t\t\t\t\t\t Total comments: {total_comments:,}')
        print("=============================== PLAYLIST INFORMATION ===============================", '\n')

    return playlist_df
