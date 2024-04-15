import json
import subprocess
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from fuzzywuzzy import fuzz
from tabulate import tabulate

roster_sheet = None


def auth(file_name='client_key.json'):
    global client

    # Authorize the API
    scope = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file'
        ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(file_name,scope)
    client = gspread.authorize(creds)


def get_players_team(player_name):
    global roster_sheet
    # Get roster sheet
    if roster_sheet is None:
        sheet = client.open('QCC 2024 Stats').worksheet('!Roster List')
        roster_sheet = sheet.get_all_records()

    player_team_map = {}
    for r in range(len(roster_sheet)):
        team = roster_sheet[r]['Team']
        players = []
        for c in range(1, 9):
            if roster_sheet[r][f'Player {c}'] != '':
                players.append(roster_sheet[r][f'Player {c}'])
        for player in players:
            player_team_map[player] = team

    # Fuzzy string match player_name to the closet key in player_team_map
    best_ratio = 0
    best_match = ''
    for player in player_team_map.keys():
        ratio = fuzz.ratio(player_name.lower(), player.lower())
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = player

    return player_team_map[best_match]


def main():
    auth()

    # If file in rehosted_replays folder
    match_folder = os.listdir('rehosted_replays')[0]
    print(f'Found {match_folder}')

    # Get match jsons
    replay_jsons = []
    for folder in os.listdir(f'rehosted_replays/{match_folder}'):
        print(f'Running r6-dissect on {folder}')
        replay_jsons.append(json.loads(subprocess.run(['./r6-dissect', f'rehosted_replays/{match_folder}/{folder}'], capture_output=True).stdout.decode('utf-8')))

    # Print round by round info
    for map_num, data in enumerate(replay_jsons):
        team_0, team_1 = '', ''
        for player in data['rounds'][0]['players']:
            if team_0 == '' and player['teamIndex'] == 0:
                team_0 = get_players_team(player['username'])
            elif team_1 == '' and player['teamIndex'] == 1:
                team_1 = get_players_team(player['username'])

        map_ = data['rounds'][0]['map']['name']
        print(f'\nMap {map_num}: {map_}')
        round_data = []
        headers = ['Round', 'ATK', 'ATK Score', 'DEF', 'DEF Score', 'Site']
        for round_ in data['rounds']:
            round_num = round_['roundNumber']

            # Sides
            attacking_team_idx = 0 if round_['teams'][0]['role'] == 'Attack' else 1
            defending_team_idx = 1 if attacking_team_idx == 0 else 0
            attacking_team = team_0 if attacking_team_idx == 0 else team_1
            defending_team = team_0 if attacking_team_idx == 1 else team_1

            # Scores
            atk_score = round_['teams'][attacking_team_idx]['score']
            def_score = round_['teams'][defending_team_idx]['score']

            # Site
            site = 'N/A'
            if 'site' in round_.keys():
                site = round_['site']

            round_data.append([round_num, attacking_team, atk_score, defending_team, def_score, site])
        print(tabulate(round_data, headers=headers, tablefmt='grid'))


if __name__ == '__main__':
    main()
