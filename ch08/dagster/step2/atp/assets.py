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
          'loser_seed': 'VARCHAR',
          'tourney_date': 'STRING'
        })
        """, [csv_files])

@asset
def atp_players_dataset(duckdb: DuckDBResource) -> None:
    base = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"
    csv_file = f"{base}/atp_players.csv"

    with duckdb.get_connection() as conn:
        conn.execute("""
        CREATE OR REPLACE TABLE players AS
        SELECT * REPLACE(
            CASE 
                WHEN dob IS NULL THEN NULL -- <.>
                WHEN SUBSTRING(CAST(dob AS VARCHAR), 5, 4) = '0000' THEN -- <.>
                    CAST(strptime(
                        CONCAT(SUBSTRING(CAST(dob AS VARCHAR), 1, 4), '0101'), 
                        '%Y%m%d'
                    ) AS date)
                ELSE 
                    CAST(strptime(dob, '%Y%m%d') AS date) -- <.>
            END AS dob
        )
        FROM read_csv_auto($1, types = {
            'dob': 'STRING'
        });
        """, [csv_file])

@asset(deps=[atp_players_dataset])
def atp_players_name_dataset(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
        ALTER TABLE players ADD COLUMN name_full VARCHAR;
        UPDATE players
        SET name_full = name_first || ' ' || name_last
        """, [])

