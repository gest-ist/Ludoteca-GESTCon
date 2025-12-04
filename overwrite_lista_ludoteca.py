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

def clear_table():
  """ Delete ALL rows in the table."""

  response = requests.get(f"https://api.baserow.io/api/database/rows/table/{BOARD_GAMES_TABLE_ID}/?user_field_names=true", headers=headers)

  if response.status_code == 200:
    rows = response.json()["results"]
    row_ids = [row["id"] for row in rows]
    print(rows)
    
    if len(row_ids) == 0:
      print("No rows to delete.")
      return True

    if row_ids:
      delete_response = requests.post(board_games_table_url + "batch-delete/", headers=headers, json={"items": row_ids})

      print(delete_response.status_code)

      if delete_response.status_code == 204:
        print(f"Deleted {len(row_ids)} rows.")
        return True
      
  return False


def import_games(csv_file):
  df = pd.read_csv(csv_file)

  name_col = next((col for col in ["Name", "name"] if col in df.columns), None)

  bgg_col = next((col for col in ["BGG ID", "BGGID", "Bgg Id", "bgg_id", "bggId" ,"bggid"] if col in df.columns), None)

  owner_col = next((col for col in ["Owner", "owner"] if col in df.columns), None)  

  if not name_col or not bgg_col or not owner_col:
    print("CSV file must contain 'Name', 'BGG ID' and 'Owner' columns.")
    return
  
  # Overwrite: clear entire table
  print("Clearing existing data...")
  if not clear_table():
    print("Failed to clear existing data.")
    return

  success = 0
  for _, row in df.iterrows():
    row_data = {
      "Name": str(row[name_col]),
      "BGG ID": int(row[bgg_col]),
      "Owner": str(row[owner_col])
    }

    response = requests.post(board_games_table_url + "?user_field_names=true", headers=headers, json=row_data)

    if response.status_code == 200:
      success += 1
    else:
      print(f" Row failed: {row_data} - {response.status_code}: {response.text}")

    time.sleep(0.2)  # To avoid hitting rate limits
  
  print(f"Imported {success}/{len(df)} games.")

if __name__ == "__main__":
  if len(sys.argv) != 2:
    print("Usage: python overwrite_lista_ludoteca.py <csv_file>")
  else:
    import_games(sys.argv[1])
