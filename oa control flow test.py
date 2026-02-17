import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
gc = gspread.authorize(creds)

sh = gc.open_by_key("1aXvAtZkdfC6PprGSRWqPcRw5vg7so2SxXHcqZPcpyvg")
ws = sh.sheet1
print(ws.get("A1"))
