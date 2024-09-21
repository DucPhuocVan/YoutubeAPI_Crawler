from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests
from include.src.extract_youtube_api import Youtube
from include.src.transform_data import Transform
from include.src.load_and_extract_s3 import S3
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

@dag(
    start_date=datetime(2024, 1, 1),
    # schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["youtube_api_crawler"],
)
def youtube_api():
    youtube = Youtube()
    transform = Transform()
    load_extract_s3 = S3()

    # Define tasks
    @task
    def channel_overview():
        youtube.build_service()
        channel_overview_df = youtube.get_channel_overview(os.environ.get('channel_id'))
        channel_overview_df = transform.drop_duplicate(channel_overview_df)
        load_extract_s3.upload_to_s3(channel_overview_df, 'channel_overview')
        load_extract_s3.load_file_into_posgres('channel_overview', ['channel_id'], 'overwrite_daily')

        return channel_overview_df

    @task
    def all_videos(channel_overview_df):
        youtube.build_service()
        print(channel_overview_df)
        all_videos_df = youtube.get_all_videos(channel_overview_df.iloc[0]['playlist_id'])
        all_videos_df = transform.drop_duplicate(all_videos_df)
        all_videos_df = transform.convert_to_datetime(all_videos_df, 'video_published_at')
        load_extract_s3.upload_to_s3(all_videos_df, 'all_videos')
        load_extract_s3.load_file_into_posgres('all_videos', ['video_id'], 'overwrite')

        return all_videos_df

    @task
    def video_details(all_videos_df):
        youtube.build_service()
        video_list = all_videos_df['video_id'].tolist()
        video_details_df = youtube.get_video_details(video_list)
        video_details_df = transform.drop_duplicate(video_details_df)
        video_details_df = transform.convert_to_datetime(video_details_df, 'published_at')
        video_details_df = transform.convert_to_seconds(video_details_df, 'video_duration')
        load_extract_s3.upload_to_s3(video_details_df, 'video_details')
        load_extract_s3.load_file_into_posgres('video_details', ['video_id'], 'overwrite_daily')

    @task
    def video_comments(all_videos_df):
        youtube.build_service()
        video_list = all_videos_df['video_id'].tolist()
        comments_df, replies_df = youtube.get_video_comments(video_list)
        comments_df = transform.drop_duplicate(comments_df)
        replies_df = transform.drop_duplicate(replies_df)
        comments_df = transform.convert_to_datetime(comments_df, 'comment_published_at')
        comments_df = transform.convert_to_datetime(comments_df, 'comment_updated_at')
        replies_df = transform.convert_to_datetime(replies_df, 'reply_published_at')
        replies_df = transform.convert_to_datetime(replies_df, 'reply_updated_at')
        load_extract_s3.upload_to_s3(comments_df, 'video_comments')
        load_extract_s3.upload_to_s3(replies_df, 'video_replies')
        load_extract_s3.load_file_into_posgres('video_comments', ['comment_id'], 'append')
        load_extract_s3.load_file_into_posgres('video_replies', ['reply_comment_id'], 'append')

    @task
    def playlists():
        youtube.build_service()
        playlist_df = youtube.get_playlists(os.environ.get('channel_id'))
        playlist_df = transform.drop_duplicate(playlist_df)
        playlist_df = transform.convert_to_datetime(playlist_df, 'published_at')
        load_extract_s3.upload_to_s3(playlist_df, 'playlist')
        load_extract_s3.load_file_into_posgres('playlist', ['playlist_id'], 'overwrite')

        return playlist_df
    
    @task
    def video_playlists(playlist_df):
        youtube.build_service()
        video_playlists = []
        for playlist_id in playlist_df['playlist_id'].tolist():
            row = youtube.get_all_videos(playlist_id)
            video_playlists.append(row)

        video_playlists_df = pd.concat(video_playlists, ignore_index=True)
        video_playlists_df = transform.drop_duplicate(video_playlists_df)
        video_playlists_df = transform.convert_to_datetime(video_playlists_df, 'video_published_at')
        load_extract_s3.upload_to_s3(video_playlists_df, 'video_playlists')
        load_extract_s3.load_file_into_posgres('video_playlists', ['video_id'], 'overwrite')

    # Task Dependencies
    channel_overview_df = channel_overview()
    playlist_df = playlists()
    all_videos_df = all_videos(channel_overview_df)
    video_details(all_videos_df)
    video_playlists(playlist_df)
    video_comments(all_videos_df)

youtube_api()