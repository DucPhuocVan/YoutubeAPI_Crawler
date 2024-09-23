SELECT
    channel_id,
    CAST(export_date AS DATE) AS d_date,
    CAST(view_count AS INT) AS view_count,
    CAST(subscriber_count AS INT) AS subscriber_count,
    CAST(video_count AS INT) AS video_count
FROM {{ source('stg', 'channel_overview') }}