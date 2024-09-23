from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests
from include.src.extract_youtube_api import Youtube
from include.src.load_and_extract_s3 import S3
import os
import pandas as pd
from dotenv import load_dotenv
from include.transform_dbt.cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior

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
    load_extract_s3 = S3()

    # Define tasks
    @task
    def channel_overview():
        youtube.build_service()
        channel_overview_df = youtube.get_channel_overview(os.environ.get('channel_id'))
        load_extract_s3.upload_to_s3(channel_overview_df, 'channel_overview')
        load_extract_s3.load_file_into_posgres('channel_overview', ['channel_id'], 'overwrite_daily')

        return channel_overview_df

    @task
    def all_videos(channel_overview_df):
        youtube.build_service()
        print(channel_overview_df)
        all_videos_df = youtube.get_all_videos(channel_overview_df.iloc[0]['playlist_id'])
        load_extract_s3.upload_to_s3(all_videos_df, 'all_videos')
        load_extract_s3.load_file_into_posgres('all_videos', ['video_id'], 'overwrite')

        return all_videos_df

    @task
    def video_details(all_videos_df):
        youtube.build_service()
        video_list = all_videos_df['video_id'].tolist()
        video_details_df = youtube.get_video_details(video_list)
        load_extract_s3.upload_to_s3(video_details_df, 'video_details')
        load_extract_s3.load_file_into_posgres('video_details', ['video_id'], 'overwrite_daily')

    @task
    def video_comments(all_videos_df):
        youtube.build_service()
        video_list = all_videos_df['video_id'].tolist()
        comments_df, replies_df = youtube.get_video_comments(video_list)
        load_extract_s3.upload_to_s3(comments_df, 'video_comments')
        load_extract_s3.upload_to_s3(replies_df, 'video_replies')
        load_extract_s3.load_file_into_posgres('video_comments', ['comment_id'], 'append')
        load_extract_s3.load_file_into_posgres('video_replies', ['reply_comment_id'], 'append')

    @task
    def playlists():
        youtube.build_service()
        playlist_df = youtube.get_playlists(os.environ.get('channel_id'))
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
        load_extract_s3.upload_to_s3(video_playlists_df, 'video_playlists')
        load_extract_s3.load_file_into_posgres('video_playlists', ['video_id'], 'overwrite')

    transform_dw = DbtTaskGroup(
        group_id='transform_dw',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/dw'],
            test_behavior=TestBehavior.AFTER_ALL
        )
    )

    # Task Dependencies
    channel_overview_df = channel_overview()
    playlist_df = playlists()
    all_videos_df = all_videos(channel_overview_df)
    [video_details(all_videos_df), video_comments(all_videos_df), video_playlists(playlist_df)] >> transform_dw

youtube_api()