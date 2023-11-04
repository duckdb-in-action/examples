from dagster_duckdb import DuckDBResource
from dagster import asset

@asset
def atp_matches_dataset(duckdb: DuckDBResource) -> None:
    base = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"
    csv_files = [ # <.>
        f"{base}/atp_matches_{year}.csv"
        for year in range(1968,2024)
    ]

    with duckdb.get_connection() as conn: # <.>
        conn.execute(""" 
        CREATE TABLE IF NOT EXISTS matches AS
        SELECT * REPLACE(
            cast(strptime(tourney_date, '%Y%m%d') AS date) as tourney_date 
        )
        FROM read_csv_auto($1, types={
          'winner_seed': 'VARCHAR', 
          'loser_seed': 'VARCHAR'
        })
        """, [csv_files])
