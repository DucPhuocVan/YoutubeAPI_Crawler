SELECT
    CAST(export_date AS DATE) AS d_date,
    CEIL(EXTRACT(DAY FROM CAST(export_date AS DATE)) / 7.0) AS week,
    EXTRACT (MONTH FROM CAST(export_date AS DATE)) AS month,
    EXTRACT (YEAR FROM CAST(export_date AS DATE)) AS year
FROM {{ source('stg', 'channel_overview') }}