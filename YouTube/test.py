# # # from youtube_api import YouTubeDataAPI
# # # from datetime import datetime
# # # import youtube_api
# # #
# # # import analytix
# # #
# # # from get_youtube_variables import get_youtube_variables
# # import pandas as pd
# #
# # from get_channel_statistics import get_channel_statistics
# # from get_channel_video_statistics import get_channel_video_statistics
# # from get_playlist_statistics import get_playlist_statistics
# #
# # # Set starting running date
# # # starting_timestamp = datetime.now()
# # # print('STARTING DATETIME: ', starting_timestamp, '\n')
# #
# # # Get YouTube credentials
# # # my_youtube_api_username, my_youtube_api_key, my_youtube_playlist_id, my_youtube_channel_id = get_youtube_variables()
# #
# # # Access YouTube API using the API key
# # # my_youtube_api = YouTubeDataAPI(my_youtube_api_key)
# #
# # # print(analytix.__version__)
# #
# # # from analytix import Analytics
# # # import datetime as dt
# # #
# # # client = Analytics.with_secrets("secrets.json")
# # # print(client)
# # #
# # # report = client.retrieve(
# # #     dimensions=("day",),
# # #     metrics=("views",),
# # #     start_date=dt.date(2022, 1, 1),
# # #     end_date=dt.date(2022, 12, 31),
# # # )
# # # print(report)
# # # df = report.to_dataframe()
# # # print(df)
# #
# #
# #
# # import datetime as dt
# #
# # import matplotlib.pyplot as plt
# # import seaborn as sns
# #
# # import analytix
# # from analytix import Analytics
# #
# # print(pd.read_json("secrets.json"))
# #
# # # if __name__ == "__main__":
# # #     analytix.setup_logging()
# # #
# # #     client = Analytics.with_secrets("secrets.json")
# # #     print(client)
# # #     report = client.retrieve(
# # #         dimensions=("day",),
# # #         # filters={"country": "CY"},
# # #         metrics=("views", "likes", "dislikes"),
# # #         start_date=dt.date(2020, 1, 1),
# # #         end_date=dt.date(2022, 12, 31),
# # #         sort_options=("-views",),
# # #     )
# # #
# # #     report.to_csv('analytics-2022.csv')
# # #     df = report.to_dataframe()
# # #     print(df)
# # #     # sns.lineplot(data=df, x="day", y="views")
# # #     # plt.savefig("daily-views-2021.png")
# #
# #
# # import os
# # import google.oauth2.credentials
# # import google_auth_oauthlib.flow
# # from googleapiclient.discovery import build
# # from googleapiclient.errors import HttpError
# # from google_auth_oauthlib.flow import InstalledAppFlow
# #
# # SCOPES = ['https://www.googleapis.com/auth/yt-analytics.readonly']
# #
# # API_SERVICE_NAME = 'youtubeAnalytics'
# # API_VERSION = 'v2'
# # CLIENT_SECRETS_FILE = "secrets.json"
# #
# # def get_service():
# #   flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
# #   credentials = flow.run_console()
# #   return build(API_SERVICE_NAME, API_VERSION, credentials = credentials)
# #
# # def execute_api_request(client_library_function, **kwargs):
# #   response = client_library_function(
# #     **kwargs
# #   ).execute()
# #
# #   print(response)
# #
# # if __name__ == '__main__':
# #   # Disable OAuthlib's HTTPs verification when running locally.
# #   # *DO NOT* leave this option enabled when running in production.
# #   os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
# #
# #   youtubeAnalytics = get_service()
# #   execute_api_request(
# #       youtubeAnalytics.reports().query,
# #       ids='channel==MINE',
# #       startDate='2017-01-01',
# #       endDate='2017-12-31',
# #       metrics='estimatedMinutesWatched,views,likes,subscribersGained',
# #       dimensions='day',
# #       sort='day'
# #   )
#
# from googleapiclient.discovery import build
# from google_auth_oauthlib.flow import InstalledAppFlow
# from google.auth.transport.requests import Request
#
# import urllib.parse as p
# import re
# import os
# import pickle
#
# SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
#
#
# def youtube_authenticate():
#     os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
#     api_service_name = "youtube"
#     api_version = "v3"
#     client_secrets_file = "secrets.json"
#     creds = None
#     # the file token.pickle stores the user's access and refresh tokens, and is
#     # created automatically when the authorization flow completes for the first time
#     if os.path.exists("token.pickle"):
#         with open("token.pickle", "rb") as token:
#             creds = pickle.load(token)
#     # if there are no (valid) credentials availablle, let the user log in.
#     if not creds or not creds.valid:
#         if creds and creds.expired and creds.refresh_token:
#             creds.refresh(Request())
#         else:
#             flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, SCOPES)
#             creds = flow.run_local_server(port=0)
#         # save the credentials for the next run
#         with open("token.pickle", "wb") as token:
#             pickle.dump(creds, token)
#
#     return build(api_service_name, api_version, credentials=creds)
#
# # authenticate to YouTube API
# youtube = youtube_authenticate()
#
#
# def get_video_id_by_url(url):
#     """
#     Return the Video ID from the video `url`
#     """
#     # split URL parts
#     parsed_url = p.urlparse(url)
#     # get the video ID by parsing the query of the URL
#     video_id = p.parse_qs(parsed_url.query).get("v")
#     if video_id:
#         return video_id[0]
#     else:
#         raise Exception(f"Wasn't able to parse video URL: {url}")
#
#
# def get_video_details(youtube, **kwargs):
#     return youtube.videos().list(
#         part="snippet,contentDetails,statistics",
#         **kwargs
#     ).execute()
#
#
# def print_video_infos(video_response):
#     items = video_response.get("items")[0]
#     for k in items.keys():
#         print(k, items[k])
#
#     # get the snippet, statistics & content details from the video response
#     snippet = items["snippet"]
#     statistics = items["statistics"]
#     content_details = items["contentDetails"]
#     # get infos from the snippet
#     channel_title = snippet["channelTitle"]
#     title = snippet["title"]
#     description = snippet["description"]
#     publish_time = snippet["publishedAt"]
#     # get stats infos
#     comment_count = statistics["commentCount"]
#     like_count = statistics["likeCount"]
#     view_count = statistics["viewCount"]
#     duration = content_details["duration"]
#     parsed_duration = re.search(f"PT(\d+H)?(\d+M)?(\d+S)", duration).groups()
#     duration_str = ""
#     for d in parsed_duration:
#         if d:
#             duration_str += f"{d[:-1]}:"
#     duration_str = duration_str.strip(":")
#
#     print(f"""\
#     Title: {title}
#     Description: {description}
#     Channel Title: {channel_title}
#     Publish time: {publish_time}
#     Duration: {duration_str}
#     Number of comments: {comment_count}
#     Number of likes: {like_count}
#     Number of views: {view_count}
#     """)
#
# # video_url = "https://www.youtube.com/watch?v=jNQXAC9IVRw&ab_channel=jawed"
# video_url = "https://www.youtube.com/watch?v=hyjx0s9Lgu0"
# # parse video ID from URL
# video_id = get_video_id_by_url(video_url)
# # make API call to get video info
# response = get_video_details(youtube, id=video_id)
# # print extracted video infos
# print_video_infos(response)






# API client library
import googleapiclient.discovery
# API information
api_service_name = "youtube"
api_version = "v3"
# DEVELOPER_KEY = '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'
DEVELOPER_KEY = 'AIzaSyDsJ8ZEC3NPig-ytr3dxdIhka0XL-sHRMw'
# API client
youtube = googleapiclient.discovery.build(api_service_name,
                                          api_version,
                                          developerKey=DEVELOPER_KEY)
# Notice that nextPageToken now is requested in 'fields' parameter
request = youtube.search().list(
        part="id,snippet",
        type='video',
        q="BridgerPay",
        videoDuration='short',
        videoDefinition='high',
        maxResults=1,
        fields="nextPageToken,items(id(videoId),snippet(publishedAt,channelId,channelTitle,title,description))"
)
response = request.execute()
print(response)


for k in response.keys():
    print(k, response[k])

request = youtube.playlistItems()
print(request)

