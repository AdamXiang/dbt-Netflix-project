{{ config(materialized = 'table') }}

WITH fct_ratings AS (
    SELECT * FROM {{ ref('fct_ratings') }}
),
seed_dates AS (
    SELECT * FROM {{ ref('seed_movie_release_dates') }}
)
SELECT
    f.*,
    CASE
        WHEN s.release_date IS NULL THEN 'UNKNOWN'
        ELSE 'KNOWN'
    END AS release_info_available
FROM
    fct_ratings f
LEFT JOIN seed_dates s
ON f.movie_id = s.movie_id
