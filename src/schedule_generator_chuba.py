import requests
import json

url = "https://api.pandascore.co/lol/matches/upcoming"
params = {
    "sort": "",
    "page": 1,
    "per_page": 100,
    "token": "CxYpySxn102vNi0zt6OmtNPqeOYrTnazKxyE9VhHQtsG5M5yW0E"
}

response = requests.get(url, params=params)
data = response.json()

if response.status_code == 200:
    for match in data:
        match_name = match["name"]
        match_datetime = match["begin_at"]
        print("Partido:", match_name)
        print("Fecha y hora:", match_datetime)
        print()  # Agrega una línea en blanco entre cada partido
else:
    print("Error al obtener los datos:", response.status_code)