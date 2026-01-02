import requests 
import pandas as pd
import sys
import time
from boardgamegeek import BGGClient

TOKEN = "wLN70XhzRxmIifblExhzuWBh1JquwzNB"
BOARD_GAMES_TABLE_ID = 760059
headers = {
  "Authorization": f"Token {TOKEN}"
  }
board_games_table_url = f"https://api.baserow.io/api/database/rows/table/{BOARD_GAMES_TABLE_ID}/"

bgg = BGGClient("2608513a-89d0-4a9d-9fff-840a2f9bc2b5")

def import_csv(csv_file, owner_name):
  df = pd.read_csv(csv_file)

  bgg_col = next((col for col in ["bgg_id"] if col in df.columns), None)
  year_col = next((col for col in ["ano"] if col in df.columns), None)
  bgg_rank_col = next((col for col in ["rank"] if col in df.columns), None)
  name_col = next((col for col in ["nome"] if col in df.columns), None)
  min_players_col = next((col for col in ["minplayers"] if col in df.columns), None)
  max_players_col = next((col for col in ["maxplayers"] if col in df.columns), None)
  best_players_col = next((col for col in ["bggbestplayers"] if col in df.columns), None)
  min_time_col = next((col for col in ["minplaytime"] if col in df.columns), None)
  max_time_col = next((col for col in ["maxplaytime"] if col in df.columns), None)
  weight_col = next((col for col in ["avgweight"] if col in df.columns), None)
  avg_score_col = next((col for col in ["average"] if col in df.columns), None)

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
  
  # print(games)

  return games

def fill_images_database(games):

  for _, row in games.iterrows():

    row_data = {
    "BGG-id" : int(row["BGG-id"]),
    "Title" : str(row["Title"])
    }

    response = requests.get(board_games_table_url + "?user_field_names=true"+ "&filter__BGG-id__equal=" + str(row_data["BGG-id"]), headers=headers)
    exists = response.json()['count'] > 0

    # print(response.json())

    if exists:
      game_has_no_img = response.json()['results'][0]['Image'] == ""
      # print(game_has_no_img)
      if game_has_no_img:
        print(f"{row_data['Title']} (BGG-id:{row_data['BGG-id']}) has no image.")
        game = bgg.game(game_id=int(row_data["BGG-id"]))
        patch_data = {
          "Image" : str(game.image)
        }

        # print(response.json()['results'][0]['id'])
        # sys.exit(0)

        patch_response = requests.patch(board_games_table_url+str(response.json()['results'][0]['id'])+"/?user_field_names=true",
                                  headers={
                                    "Authorization": f"Token {TOKEN}",
                                    "Content-Type": "application/json"
                                  },
                                  json=patch_data)

        if patch_response.status_code == 200:
          print("Added image successfully!")
        else:
          print(f"Failed to add image. Code: {patch_response.status_code}")

      
    else:
      print(f"Game does not exist. Skipping...")
      continue

    time.sleep(0.2)

  print(f"Finished adding games.")


if __name__ == "__main__":
  fill_images_database(import_csv(sys.argv[1], sys.argv[2]))