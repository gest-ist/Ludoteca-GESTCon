import re
import csv

input_file = "ludoteca_vasco.txt"
output_file = "ludoteca_vasco.csv"

pattern = re.compile(r'title="([^"]+)"[^>]*bgg_id=(\d+)')

rows = []

with open(input_file, "r", encoding="utf-8") as f:
  for line in f:
    match = pattern.search(line)
    if match:
      name = match.group(1)
      bgg_id = match.group(2)
      owner = "JogoNaMesa"
      rows.append([name, bgg_id, owner])


# Write to CSV
with open(output_file, "w", newline="", encoding="utf-8") as f:
  writer = csv.writer(f)
  writer.writerow(["Name", "BGG ID", "Owner"])
  writer.writerows(rows)