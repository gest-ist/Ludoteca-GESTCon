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

def get_best_player_count(ps):

   # Get best player count (according to users votes in bgg)
  arr = [x._data['best'] for x in ps]
  best_idx = arr.index(max(arr))
  best_player_count = [ps[best_idx]._data['player_count']]
  arr2 = arr
  arr2.pop(best_idx)
  best2_idx = arr2.index(max(arr2))
  total_votes_best2 = ps[best2_idx]._data['best'] + ps[best2_idx]._data['recommended'] + ps[best2_idx]._data['not_recommended']
  if total_votes_best2 == 0:
    percentage = 0
  else:
    percentage = ps[best2_idx]._data['best']/total_votes_best2 * 100
  if percentage >= 50:
    best_player_count.append(ps[best2_idx]._data['player_count'])
    best_player_count.sort()
    
  best_player_count = ",".join(best_player_count)
  # print(best_player_count)

  return best_player_count

def query_bgg_game(bgg_id, owner):
  
  game = bgg.game(game_id=bgg_id)

  data = {
    "BGG-id": int(game.id),
    "Title" : str(game.name),
    "Image" : str(game.image),
    # "Thumb" : str(game.thumbnail),
    "Publishing-year": int(game._year_published),
    "Players-min": int(game.min_players),
    "Players-max": int(game.max_players),
    "Players-best": str(get_best_player_count(game.player_suggestions)),
    "Time-min": int(game.min_playing_time),
    "Time-max": int(game.max_playing_time),
    "Weight": round(float(game.rating_average_weight), 2),
    "Avg-score": round(float(game.rating_average), 2),
    "Owner": str(owner)
  }

  # print(data)

  return data

def append_imcomplete_csv(games):

  for _, row in games.iterrows():

    row_data = {
    "BGG-id" : int(row["BGG-id"]),
    "Title" : str(row["Title"])
    }

    response = requests.get(board_games_table_url + "?user_field_names=true"+ "&filter__BGG-id__equal=" + str(row_data["BGG-id"]), headers=headers)
    exists = response.json()['count'] > 0

    if exists:
      no_entries_db = response.json()['count']
      print(f" {row_data['Title']} (BGG-id:{row_data['BGG-id']}) has {no_entries_db} entries in the database already.")
      continue
    else:
      print(f"Game does not exist. Adding {row_data['Title']}...")
      row_data = query_bgg_game(row_data["BGG-id"], sys.argv[2])

      response = requests.post(board_games_table_url+"?user_field_names=true", headers=headers, json=row_data)

      if response.status_code == 200:
        print(f"Added {row_data['Title']} (BGG-id:{row_data['BGG-id']})")
      else:
        print(f"Failed to add {row_data['Title']}. Code {response.status_code}: {response.text}")

    time.sleep(0.2)

  print(f"Finished adding games.")


if __name__ == "__main__":
  if len(sys.argv) != 3:
    print("Usage: python append_imcomplete_csv.py <csv_file> <owner>")
    sys.exit(1)
  else:
    append_imcomplete_csv(import_csv(sys.argv[1], sys.argv[2]))
    # import_csv(sys.argv[1], sys.argv[2])
    # TO BE TESTED STILL
    # query_bgg_game(24517, "JogoNaMesa")
    # query_bgg_game(445,"JogoNaMesa")
    # query_bgg_game(14126,"JogoNaMesa")
