{{ config(
    materialized='external', 
    location='output/matches.parquet', 
    format='parquet'
) }} -- <.>

WITH noWinLoss AS (
    SELECT COLUMNS(col -> 
      NOT  regexp_matches(col, 'w_.*') AND -- <.>
      NOT regexp_matches(col, 'l_.*') -- <.>
    ) 
    FROM {{ source('github', 'matches_file') }}  -- <.>
)

SELECT * REPLACE (
    cast(strptime(tourney_date, '%Y%m%d') AS date) as tourney_date -- <.>
)
FROM noWinLoss
