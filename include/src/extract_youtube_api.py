from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
import pandas as pd
import os
from dotenv import load_dotenv
from .checkpoint import CheckPoint
from datetime import datetime, timezone
import isodate

load_dotenv()

class Youtube:
    def __init__(self):
        self.api_service_name = "youtube"
        self.api_version = "v3"
        self.youtube = None
        self.checkpoint = CheckPoint()

    def build_service(self):
        self.youtube = build(
            self.api_service_name, 
            self.api_version, 
            developerKey=os.environ.get("api_key")
        )
        
    def get_channel_overview(self, channel_id):
        if not self.youtube:
            raise ValueError("YouTube service is not built")
        channel_overview = []
        request = self.youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()

        for item in response['items']:
            row = {
                'channel_id': item['id'],
                'channel_name': item['snippet']['title'],
                'description': item['snippet']['description'],
                'published_at': item['snippet']['publishedAt'],
                'country': item['snippet']['country'],
                'view_count': item['statistics']['viewCount'],
                'subscriber_count': item['statistics']['subscriberCount'],
                'video_count': item['statistics']['videoCount'],
                'playlist_id': item['contentDetails']['relatedPlaylists']['uploads']
            }
            channel_overview.append(row)

        df = pd.DataFrame(channel_overview)

        return df
    
    def get_all_videos(self, playlist_id):
        if not self.youtube:
            raise ValueError("YouTube service is not built")
        videos = []
        request = self.youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults = 50
        )
        while request:
            response = request.execute()
            
            for item in response['items']:
                row = {
                    'video_id': item['contentDetails']['videoId'],
                    'video_published_at': item['contentDetails'].get('videoPublishedAt', 'N/A'),
                    'video_title': item['snippet']['title'],
                    'video_description': item['snippet']['description'],
                    'playlist_id': item['snippet']['playlistId']
                }
                videos.append(row)

            request = self.youtube.playlistItems().list_next(request, response)

        df = pd.DataFrame(videos)
        return df
    
    def get_video_details(self, video_list):
        if not self.youtube:
            raise ValueError("YouTube service is not built")
        video = []    
        for i in range(0, len(video_list), 50):
            request = self.youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_list[i: i+50]
            )

            response = request.execute()

            for item in response['items']:
                row = {
                    'video_id': item['id'],
                    'published_at': item['snippet']['publishedAt'],
                    'video_title': item['snippet']['title'],
                    'video_description': item['snippet']['description'],
                    'category_id': item['snippet']['categoryId'],
                    'video_duration': int(isodate.parse_duration(item['contentDetails']['duration']).total_seconds()),
                    'video_view_count': item['statistics']['viewCount'],
                    'video_like_count': item['statistics']['likeCount'],
                    'video_comment_count': item['statistics']['commentCount']
                }
                video.append(row)

        df = pd.DataFrame(video)
        return df
    
    def get_video_comments(self, video_list):
        if not self.youtube:
            raise ValueError("YouTube service is not built")
        
        all_comments = []
        all_replies = []

        # create table if not exists
        self.checkpoint.create_checkpoint_table()

        # get checkpoint
        checkpoint_comment = self.checkpoint.get_last_checkpoint("video_comments")
        checkpoint_reply = self.checkpoint.get_last_checkpoint("video_replies")

        # Ensure checkpoints are timezone-aware (assuming they are stored without timezone)
        if checkpoint_comment and checkpoint_comment.tzinfo is None:
            checkpoint_comment = checkpoint_comment.replace(tzinfo=timezone.utc)
        
        if checkpoint_reply and checkpoint_reply.tzinfo is None:
            checkpoint_reply = checkpoint_reply.replace(tzinfo=timezone.utc)

        for video_id in video_list:
            try:
                next_page_token = None

                while True:
                    request = self.youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=video_id,
                        maxResults=100,
                        pageToken=next_page_token
                    )
                    response = request.execute()

                    # Load comment
                    for item in response['items']:
                        comment_updated_at = item['snippet']['topLevelComment']['snippet']['updatedAt']
                        comment_updated_at_dt = datetime.strptime(comment_updated_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

                        # Skip comment <= checkpoint
                        if checkpoint_comment and comment_updated_at_dt <= checkpoint_comment:
                            continue

                        row = {
                            'comment_id': item['id'],
                            'channel_id': item['snippet']['channelId'],
                            'video_id': item['snippet']['videoId'],
                            'comment_text_display': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            'comment_author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'comment_author_url': item['snippet']['topLevelComment']['snippet']['authorChannelUrl'],
                            'comment_author_channel_id': item['snippet']['topLevelComment']['snippet']['authorChannelId']['value'],
                            'comment_like_count': item['snippet']['topLevelComment']['snippet']['likeCount'],
                            'comment_published_at': item['snippet']['topLevelComment']['snippet']['publishedAt'],
                            'comment_updated_at': item['snippet']['topLevelComment']['snippet']['updatedAt'],
                            'comment_reply_count': item['snippet']['totalReplyCount']
                        }
                        all_comments.append(row)

                        # Load replies
                        if 'replies' in item:
                            for reply in item['replies']['comments']:
                                reply_updated_at = reply['snippet']['updatedAt']
                                reply_updated_at_dt = datetime.strptime(reply_updated_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

                                # Skip reply <= checkpoint
                                if checkpoint_reply and reply_updated_at_dt <= checkpoint_reply:
                                    continue

                                row_reply = {
                                    'reply_comment_id': reply['id'],
                                    'comment_id_parrent': reply['snippet']['parentId'],
                                    'reply_text_display': reply['snippet']['textDisplay'],
                                    'reply_author': reply['snippet']['authorDisplayName'],
                                    'reply_author_url': reply['snippet']['authorChannelUrl'],
                                    'reply_channel_id': reply['snippet']['authorChannelId']['value'],
                                    'reply_like_count': reply['snippet']['likeCount'],
                                    'reply_published_at': reply['snippet']['publishedAt'],
                                    'reply_updated_at': reply['snippet']['updatedAt']
                                }
                                all_replies.append(row_reply)

                    next_page_token = response.get('nextPageToken')
                    if not next_page_token:
                        break

            except HttpError as e:
                print(f"An error occurred for video ID {video_id}: {e}")

        # Convert to DataFrame
        comments_df = pd.DataFrame(all_comments)
        replies_df = pd.DataFrame(all_replies)

        # Update checkpoint date to the latest `updated_at`
        if not comments_df.empty:
            latest_comment_date = pd.to_datetime(comments_df['comment_updated_at']).max().replace(tzinfo=timezone.utc)
        else:
            latest_comment_date = checkpoint_comment

        if not replies_df.empty:
            latest_reply_date = pd.to_datetime(replies_df['reply_updated_at']).max().replace(tzinfo=timezone.utc)
        else:
            latest_reply_date = checkpoint_reply

        # Update checkpoint in the database
        self.checkpoint.update_checkpoint("video_comments", latest_comment_date)
        self.checkpoint.update_checkpoint("video_replies", latest_reply_date)

        return comments_df, replies_df
    
    def get_playlists(self, channel_id):
        if not self.youtube:
            raise ValueError("YouTube service is not built")
        playlists = []
        request = self.youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=25
        )
        response = request.execute()

        for item in response['items']:
            row = {
                'playlist_id': item['id'],
                'published_at': item['snippet']['publishedAt'],
                'channel_id': item['snippet']['channelId'],
                'playlist_title': item['snippet']['title']
            }
            playlists.append(row)

        df = pd.DataFrame(playlists)
        return df