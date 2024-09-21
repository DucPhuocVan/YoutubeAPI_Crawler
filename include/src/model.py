from pydantic import BaseModel
from datetime import datetime
from typing import Optional
    
class ChannelOverview(BaseModel):
    channel_id: Optional[str] = None
    channel_name: Optional[str] = None
    description: Optional[str] = None
    published_at: Optional[datetime] = None
    country: Optional[str] = None
    view_count: Optional[int] = None
    subscriber_count: Optional[int] = None
    video_count: Optional[int] = None
    playlist_id: Optional[str] = None

class AllVideos(BaseModel):
    video_id: Optional[str] = None
    video_published_at: Optional[datetime] = None
    video_title: Optional[str] = None
    video_description: Optional[str] = None
    playlist_id: Optional[str] = None

class Playlist(BaseModel):
    playlist_id: Optional[str] = None
    published_at: Optional[datetime] = None
    channel_id: Optional[str] = None
    playlist_title: Optional[str] = None

class VideoDetails(BaseModel):
    video_id: Optional[str] = None
    published_at: Optional[datetime] = None
    video_title: Optional[str] = None
    video_description: Optional[str] = None
    category_id: Optional[str] = None
    video_duration: Optional[int] = None
    video_view_count: Optional[int] = None
    video_like_count: Optional[int] = None
    video_comment_count: Optional[int] = None

class VideoPlaylists(BaseModel):
    video_id: Optional[str] = None
    video_published_at: Optional[datetime] = None
    video_title: Optional[str] = None
    video_description: Optional[str] = None
    playlist_id: Optional[str] = None

class VideoComments(BaseModel):
    comment_id: Optional[str] = None
    channel_id: Optional[str] = None
    video_id: Optional[str] = None
    comment_text_display: Optional[str] = None
    comment_author: Optional[str] = None
    comment_author_url: Optional[str] = None
    comment_author_channel_id: Optional[str] = None
    comment_like_count: Optional[int] = None
    comment_published_at: Optional[datetime] = None
    comment_updated_at: Optional[datetime] = None
    comment_reply_count: Optional[int] = None

class VideoReplies(BaseModel):
    reply_comment_id: Optional[str] = None
    comment_id_parrent: Optional[str] = None
    reply_text_display: Optional[str] = None
    reply_author: Optional[str] = None
    reply_author_url: Optional[str] = None
    reply_channel_id: Optional[str] = None
    reply_like_count: Optional[int] = None
    reply_published_at: Optional[datetime] = None
    reply_updated_at: Optional[datetime] = None