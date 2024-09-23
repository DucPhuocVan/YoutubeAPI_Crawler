SELECT DISTINCT
    channel_id,
    channel_name,
    description,
    CAST(published_at AS DATE) AS published_at,
    country
FROM {{ source('stg', 'channel_overview') }}