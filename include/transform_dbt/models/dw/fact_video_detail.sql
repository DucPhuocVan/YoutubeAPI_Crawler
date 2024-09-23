SELECT
    video_id,
    CAST(export_date AS DATE) AS d_date,
    CAST(video_view_count AS INT) AS video_view_count,
    CAST(video_like_count AS INT) AS video_like_count,
    CAST(video_comment_count AS INT) AS video_comment_count
FROM {{ source('stg', 'video_details') }}