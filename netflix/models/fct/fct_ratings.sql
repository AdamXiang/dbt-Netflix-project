{# if the schema of source table has changed, we will not be able to incrementally update       #}

{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
    )
}}
WITH src_ratings AS (
    SELECT * FROM {{ ref('src_ratings') }}
)
SELECT
    user_id,
    movie_id,
    rating,
    rating_timestamp
FROM
    src_ratings
WHERE
    rating IS NOT NULL

{% if is_incremental() %}
    AND
    rating_timestamp > (SELECT MAX(rating_timestamp) FROM {{ this }})
{% endif %}


{#
    src_rating (base table) has a new record -> at 6 PM
    fct_ratings (fct table) max timestamp -> 5 PM
    this refers to fct_ratings
#}