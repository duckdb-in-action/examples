import dlt
from chess import source

pipeline = dlt.pipeline(
    pipeline_name="chess_pipeline", # <.>
    destination="duckdb",
    dataset_name="main" # <.>
)

data = source( # <.>
    players=[ # <.>
      "magnuscarlsen", "vincentkeymer",
      "dommarajugukesh", "rpragchess"
    ],
    start_month="2022/11",
    end_month="2022/11",
)

players_profiles_and_games = data.with_resources("players_profiles", "players_games")

info = pipeline.run(players_profiles_and_games)
print(info)
