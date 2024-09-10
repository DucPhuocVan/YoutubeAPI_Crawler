Project: YouTube API Data Crawler
========


Project Overview
================

- This project involves crawling data from the YouTube API.
- Data is ingested from YouTube using the API.
- The data is saved into CSV files and then uploaded to MinIO.
- The data is also loaded into a PostgreSQL database.

Information channel crawler
================

- Channel name: Duy Luân Dễ Thương
- URL: https://www.youtube.com/@duyluandethuong
- Content crawler:
    - Channel overview
    - All videos
    - Playlists
    - Video in Playlist
    - Video Details
    - Video Comments (include Replies)

Architecture
================
![alt text](include/architecture.drawio.png)

