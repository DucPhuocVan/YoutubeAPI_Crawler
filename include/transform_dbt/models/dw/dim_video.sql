WITH cte_get_playlist AS (
    SELECT DISTINCT
        all_vid.video_id,
        CAST(all_vid.video_published_at AS DATE) AS video_published_at,
        all_vid.video_title,
        all_vid.video_description,
        CASE 
            WHEN playlist_vid.video_id IS NOT NULL THEN playlist_vid.playlist_id 
            ELSE all_vid.playlist_id
        END playlist_id
    FROM {{ source('stg', 'all_videos') }} all_vid
    LEFT JOIN {{ source('stg', 'video_playlists') }} playlist_vid
        ON all_vid.video_id = playlist_vid.video_id
)
SELECT DISTINCT
    video.video_id,
    video.video_published_at,
    video.video_title,
    video.video_description,
    CAST(vid_detail.video_duration AS INT) AS video_duration,
    video.playlist_id,
    CASE 
        WHEN playlist.playlist_id IS NOT NULL THEN playlist.playlist_title
        ELSE 'Other'
    END playlist_title
FROM cte_get_playlist video
LEFT JOIN {{ source('stg', 'playlist') }} playlist
    ON video.playlist_id = playlist.playlist_id
LEFT JOIN {{ source('stg', 'video_details') }} vid_detail
    ON video.video_id = vid_detail.video_id