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

""" This function reads an exported BGG collection csv, """
""" and returns a DataFrame with the relevant columns and the owner name added."""

def import_csv(csv_file, owner_name):
  df = pd.read_csv(csv_file)

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

  return games

""" This function checks if the items in the games list exist in the database."""
""" If they do, it marks them as available if they are not already."""

def mark_as_available(games, owner_name):

  made_available = 0
  for _, row in games.iterrows():
    # check if game already exists in Baserow by BGG-id

    bgg_id = str(row["BGG-id"])
    owner = owner_name

    response = requests.get(board_games_table_url + "?user_field_names=true&filter__BGG-id__equal=" + bgg_id + "&filter__Owner__equal=" + owner, headers=headers)
    #print(response.status_code)
    if response.status_code != 200:
      print("GET query did not return 200.")
      sys.exit(1)

    exists = response.json()['count'] > 0

    if exists == 0: # If there's no entry, prompt the user to add it to the DB
      print(f"Game with BGG-id {row['BGG-id']} does not exist in the database.")
      print("Would you like to add it and mark it as available? (y/n) ")
      choice = input().lower()
      while choice not in ['y', 'n']:
        print("Please enter 'y' or 'n': ")
        choice = input().lower()
      if choice == 'n':
        continue
      elif choice == 'y':
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
          "Owner": str(row["Owner"]),
          "Available": True
        }

        response = requests.post(board_games_table_url + "?user_field_names=true", headers=headers, json=row_data)

        if response.status_code == 200:
          print(f"Added {row_data['Title']} and marked as available. Owned by {row_data["Owner"]}")
        else:
          print(f" Failed to add: {row_data['Title']} - {response.status_code}: {response.text}")
    
    else: # If there's one or more entries
      # This assumes that the number of entries in the database is the same as the ones given in the csv file

      no_entries_db = response.json()['count']

      unavailable_count = sum(not item['Available'] for item in response.json()['results'])

      if unavailable_count == 0: # All entries for this game and owner are already marked as available
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
            "Owner": str(row["Owner"]),
            "Available": True
          }

          response = requests.post(board_games_table_url + "?user_field_names=true", headers=headers, json=row_data)

          if response.status_code == 200:
            print(f"Added {row_data['Title']} and marked as available. Owned by {row_data["Owner"]}")
          else:
            print(f" Failed to add: {row_data['Title']} - {response.status_code}: {response.text}")

      else: # If there are any entries marked as unavailable 
        for i, entry in enumerate(response.json()['results']):
          if entry['Available'] == False:
            patch_data = {
              "Available": True
            }
            patch_response = requests.patch(board_games_table_url+str(entry["id"])+"/?user_field_names=true",
                                            headers={
                                              "Authorization": f"Token {TOKEN}",
                                              "Content-Type": "application/json"
                                            },
                                            json=patch_data)
            
            #print(patch_response.json())

            if patch_response.status_code == 200:
              made_available += 1
              print(f"Marked {entry['Title']} (BGG-id: {entry['BGG-id']}) as available. (Entry {i+1}/{no_entries_db})")
            else:
              print(f"Failed to mark {entry['Title']} (BGG-id: {entry['BGG-id']}) as available.")
            
            break # break out of the loop after trying once to patch the entry to available
        

  print(f"Marked {made_available} games as available.")



if __name__ == "__main__":
  if len(sys.argv) < 3:
    print("Usage: python mark_as_available.py <csv_file> <owner_name>")
    sys.exit(1)
  else:
    mark_as_available(import_csv(sys.argv[1], sys.argv[2]), sys.argv[2])
