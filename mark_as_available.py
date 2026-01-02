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

""" This function reads an exported BGG collection csv, """
""" and returns a DataFrame with the relevant columns and the owner name added."""

def import_csv(csv_file):
  df = pd.read_csv(csv_file, encoding='latin-1')

  name_col = next((col for col in ["Title"] if col in df.columns), None)
  bgg_col = next((col for col in ["BGG-id"] if col in df.columns), None)
  year_col = next((col for col in ["Publishing-year"] if col in df.columns), None)
  min_players_col = next((col for col in ["Players-min"] if col in df.columns), None)
  max_players_col = next((col for col in ["Players-max"] if col in df.columns), None)
  best_players_col = next((col for col in ["Players-best"] if col in df.columns), None)
  min_time_col = next((col for col in ["Time-min"] if col in df.columns), None)
  max_time_col = next((col for col in ["Time-max"] if col in df.columns), None)
  weight_col = next((col for col in ["Weight"] if col in df.columns), None)
  avg_score_col = next((col for col in ["Avg-score"] if col in df.columns), None)
  owner_name = next((col for col in ["Owner"] if col in df.columns), None)

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
    "Owner": df[owner_name] if owner_name else None
  })

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


""" This function checks if the items in the games list exist in the database."""
""" If they do, it marks them as available if they are not already."""

def mark_as_available(games, skip_existing=False):

  made_available = 0
  for _, row in games.iterrows():
    # check if game already exists in Baserow by BGG-id

    bgg_id = str(row["BGG-id"])

    if bgg_id == "nan":
      title = str(row["Title"])
      response = requests.get(board_games_table_url + "?user_field_names=true&filter__title__equal=" + title, headers=headers)

      if response.status_code != 200:
        print("GET query did not return 200.")
        sys.exit(1)

    else:
      response = requests.get(board_games_table_url + "?user_field_names=true&filter__bggId__equal=" + bgg_id, headers=headers)
      #print(response.status_code)
      if response.status_code != 200:
        print("GET query did not return 200.")
        sys.exit(1)

    exists = response.json()['count'] > 0

    if exists == 0: # If there's no entry, prompt the user to add it to the DB
      # if not skip_existing:
      print(f"{row["Title"]} ({row['BGG-id']}) does not exist in the database.")
      print("Would you like to add it and mark it as available? (y/n) ")
      choice = input().lower()
      while choice not in ['y', 'n']:
        print("Please enter 'y' or 'n': ")
        choice = input().lower()
      if choice == 'n':
        continue
      elif choice == 'y':
        if bgg_id != "nan":
         
         game = bgg.game(game_id=int(float(bgg_id)))
        #  time.sleep(1)  # To avoid hitting BGG rate limits

        else:
          game = bgg.game(name=str(row["Title"]))
          # time.sleep(1)  # To avoid hitting BGG rate limits

        row_data = {
          "title": str(game.name),
          "bggId": int(game.id),
          "image": str(game.image),
          "publishingYear": int(game._year_published),
          "playersMin": int(game.min_players),
          "playersMax": int(game.max_players),
          "playersBest": str(get_best_player_count(game.player_suggestions)),
          "timeMin": int(game.min_playing_time),
          "timeMax": int(game.max_playing_time),
          "weight": round(float(game.rating_average_weight), 2),
          "avgScore": round(float(game.rating_average), 2),
          "owner": str(row["Owner"]),
          "status": "Available"
        }

        response = requests.post(board_games_table_url + "?user_field_names=true", headers=headers, json=row_data)

        if response.status_code == 200:
          print(f"Added {row_data['title']} and marked as available. Owned by {row_data["owner"]}")
        else:
          print(f" Failed to add: {row_data['title']} - {response.status_code}: {response.text}")

    else: # If there's one or more entries
      # This assumes that the number of entries in the database are the same as the ones given in the csv file

      no_entries_db = response.json()['count']

      unavailable_count = sum(item['status']['value']=="Unavailable" for item in response.json()['results'])
      # print(unavailable_count)

      if unavailable_count == 0: # All entries for this game and owner are already marked as available
        if not skip_existing:
          # Prompt the user to add another entry for this game to the database 
          print(f"All entries ({no_entries_db}) for {row['Title']} (BGG-id {row['BGG-id']}) are already marked as available.")
          print("Would you like to add ANOTHER entry and mark it as available? (y/n) ")
          while True:  
            choice = input().lower()
            if choice in ['y', 'n']:
              break
            print("Please enter 'y' or 'n'")
        
          if choice == 'n':
            continue
          elif choice == 'y':
            row_data = {
              "title": str(row["Title"]),
              "bggId": int(row["BGG-id"]),
              "publishingYear": int(row["Publishing-year"]),
              "playersMin": int(row["Players-min"]),
              "playersMax": int(row["Players-max"]),
              "playersBest": str(row["Players-best"]),
              "timeMin": int(row["Time-min"]),
              "timeMax": int(row["Time-max"]),
              "weight": round(float(row["Weight"]), 2),
              "avgScore": round(float(row["Avg-score"]), 2),
              "owner": str(row["Owner"]),
              "status": "Available"
            }

            response = requests.post(board_games_table_url + "?user_field_names=true", headers=headers, json=row_data)

            if response.status_code == 200:
              print(f"Added {row_data['title']} and marked as available. Owned by {row_data['owner']}")
            else:
              print(f" Failed to add: {row_data['title']} - {response.status_code}: {response.text}")

      else: # If there are any entries marked as unavailable 
        for i, entry in enumerate(response.json()['results']):
          if entry['owner'] != row['Owner']:
            var = row['Owner']
            patch_data = {
              "owner": str(var),
            }

          if entry['status']['value'] == "Unavailable":
            # concatenate patch data to mark as available
            patch_data["status"] = "Available"
            # print(patch_data)
            patch_response = requests.patch(board_games_table_url+str(entry["id"])+"/?user_field_names=true",
                                            headers={
                                              "Authorization": f"Token {TOKEN}",
                                              "Content-Type": "application/json"
                                            },
                                            json=patch_data)
            
            #print(patch_response.json())

            if patch_response.status_code == 200:
              made_available += 1
              print(f"Marked {entry['title']} (BGG-id: {entry['bggId']}), owned by {row['Owner']} as available. (Entry {i+1}/{no_entries_db})")
            else:
              print(f"Failed to mark {entry['title']} (BGG-id: {entry['bggId']}) as available.")

            break # break out of the loop after trying once to patch the entry to available
        

  print(f"Marked {made_available} games as available.")



if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("Usage: python mark_as_available.py <csv_file>")
    sys.exit(1)
  else:
    if len(sys.argv) == 3:
      if sys.argv[2] == "--skip-existing" or sys.argv[2] == "-s":
        mark_as_available(import_csv(sys.argv[1]), skip_existing=True)
    else:
      mark_as_available(import_csv(sys.argv[1]))