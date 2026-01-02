import requests
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

def query_bgg_game(bgg_id):
  
  game = bgg.game(game_id=bgg_id)

  data = {
    "BGG-id": int(game.id),
    "Title" : str(game.name),
    "Image" : str(game.image),
    # "Thumb" : str(game.thumbnail),
    "Publishing-year": int(game._year_published),
    "Players-min": int(game.min_players),
    "Players-max": int(game.max_players),
    # "Players-best": str(get_best_player_count(game.player_suggestions)),
    "Time-min": int(game.min_playing_time),
    "Time-max": int(game.max_playing_time),
    "Weight": round(float(game.rating_average_weight), 2),
    "Avg-score": round(float(game.rating_average), 2)#,
    # "Owner": str(owner)
  }

  # print(data)

  return data

def get_database_rows(page):

  response = requests.get(board_games_table_url+f"?user_field_names=true&page={page}&include=BGG-id", headers=headers)

  if not response.status_code == 200:
    print(f"Failed to obtain database rows. Code {response.status_code}")
    return
  
  return response.json()['results']

def overwrite_names_to_english(rows, starting_page):
  
  # The database has N rows. By default, listing rows returns only 100 at a time

  page_size = 100
  total_iter = int(rows / page_size) + (rows % page_size > 0)

  page = starting_page - 1
  while page < total_iter:
    page += 1
    games = get_database_rows(page)
    
    for game in games:
      
      game_data = query_bgg_game(game['BGG-id'])

      patch_data = {
        "Title" : game_data['Title']
      }
      
      patch_response = requests.patch(board_games_table_url+str(game['id'])+"/?user_field_names=true",
                                      headers={
                                        "Authorization": f"Token {TOKEN}",
                                        "Content-Type": "application/json"
                                      },
                                      json=patch_data)

      if not patch_response.status_code == 200:
        print(f"Failed to patch name of {game_data['Title']} (BGG-id: {game_data['BGG-id']}). Code {patch_response.status_code}")
        continue
      else:
        print(f"Successfully patched name of {game_data['Title']} (BGG-id: {game_data['BGG-id']})")

      time.sleep(5)

if __name__ == "__main__":
  overwrite_names_to_english(2000, 7)
