import requests
import pandas as pd
import sys
import time

TOKEN = "wLN70XhzRxmIifblExhzuWBh1JquwzNB"
BOARD_GAMES_TABLE_ID = 760059
headers = {
  "Authorization": f"Token {TOKEN}"
  }
board_games_table_url = f"https://api.baserow.io/api/database/rows/table/{BOARD_GAMES_TABLE_ID}/"

def read_games_from_csv(csv_file, owner_name):
  df = pd.read_csv(csv_file)

  # Extract columns from csv. A bgg collection csv has a lot of columns
  # The choice of columns here was arbitrary and manual

  name_col = next((col for col in ["objectname"] if col in df.columns), None)
  bgg_col = next((col for col in ["objectid"] if col in df.columns), None)
  year_col = next((col for col in ["yearpublished"] if col in df.columns), None)
  min_players_col = next((col for col in ["minplayers"] if col in df.columns), None)
  max_players_col = next((col for col in ["maxplayers"] if col in df.columns), None)
  best_players_col = next((col for col in ["bggbestplayers"] if col in df.columns), None)
  min_time_col = next((col for col in ["minplaytime"] if col in df.columns), None)
  max_time_col = next((col for col in ["maxplaytime"] if col in df.columns), None)
  weight_col = next((col for col in ["avgweight"] if col in df.columns), None)
  avg_score_col = next((col for col in ["average"] if col in df.columns), None)

  # # create a games list with just the data above
  games = pd.DataFrame({
    "Title": df[name_col] if name_col else None,
    "BGG-id": df[bgg_col] if bgg_col else None,
    "Publishing-year": df[year_col] if year_col else None,
    "Players-min": df[min_players_col] if min_players_col else None,
    "Players-max": df[max_players_col] if max_players_col else None,
    "Players-best": df[best_players_col] if best_players_col else None,
    "Time-min": df[min_time_col] if min_time_col else None,
    "Time-max": df[max_time_col] if max_time_col else None,
    "Weight": df[weight_col] if weight_col else None,
    "Avg-score": df[avg_score_col] if avg_score_col else None,
    "Owner": owner_name
  })

  # Now, import games into Baserow
  success = 0
  for _, row in games.iterrows():
    row_data = {
      "Title": str(row["Title"]),
      "BGG-id": int(row["BGG-id"]),
      "Publishing-year": int(row["Publishing-year"]),
      "Players-min": int(row["Players-min"]),
      "Players-max": int(row["Players-max"]),
      "Players-best": str(row["Players-best"]),
      "Time-min": int(row["Time-min"]),
      "Time-max": int(row["Time-max"]),
      "Weight": round(float(row["Weight"]), 2),
      "Avg-score": round(float(row["Avg-score"]), 2),
      "Owner": str(row["Owner"])
    }

    response = requests.post(board_games_table_url + "?user_field_names=true", headers=headers, json=row_data)

    if response.status_code == 200:
      success += 1
    else:
      print(f" Row failed: {row_data} - {response.status_code}: {response.text}")

    time.sleep(0.2)  # To avoid hitting rate limits
  
  print(f"Imported {success}/{len(games)} games.")

if __name__ == "__main__":
  if len(sys.argv) < 3:
    print("Usage: python update_lista_ludoteca.py <csv_file> <owner>")
    sys.exit(1)
  else: 
    read_games_from_csv(sys.argv[1], sys.argv[2])