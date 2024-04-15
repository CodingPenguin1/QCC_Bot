import subprocess
import os
import pandas as pd
import zipfile
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from time import sleep, time
from datetime import datetime
from colorama import Fore
from fuzzywuzzy import fuzz
import shutil

INFO = f'{Fore.GREEN}[INF]{Fore.RESET} '
WARN = f'{Fore.YELLOW}[WRN]{Fore.RESET} '
ERROR = f'{Fore.RED}[ERR]{Fore.RESET} '
ACTION = f'{Fore.CYAN}[ACT]{Fore.RESET} '

client = None
roster_sheet = None


def auth(file_name='client_key.json'):
    global client

    print(INFO + 'Authenticating with Google Sheets...', end=' ')
    # Authorize the API
    scope = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file'
        ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(file_name,scope)
    client = gspread.authorize(creds)
    print('Done')


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

    if best_match != player_name:
        found = False
        for r in range(len(roster_sheet)):
            for c in range(1, 9):
                if roster_sheet[r][f'Player {c}'] == best_match:
                    found = True
                    break
            if found:
                break
        row = r + 2
        col = chr(c + 65)
        print(WARN + f'       {player_name} is marked as {best_match} in the roster list sheet')
        print(ACTION + f'       Resolution: Update "{best_match}" on the sheet \'!Roster List\'!{col}{row} to "{player_name}" (RELOAD CODE!)')

    return player_team_map[best_match]


def parse_file(file):
    # === Unzip file in replay_buffer to replay_cache ===
    while True:
        try:
            if file.endswith('.zip'):
                # Unzip to 'replay_cache' directory
                print(INFO + f'New file detected - {file}')
                with zipfile.ZipFile('cache/replay_buffer/' + file, 'r') as z:
                    z.extractall('cache/replay_cache')
                print(INFO + f'   Extracted {file} to replay_cache')
                # Save zip to data/match_replays
                if not os.path.exists('data/match_replays/' + file):
                    print(INFO + f'   Saving {file} to match_replays')
                    os.rename('cache/replay_buffer/' + file, 'data/match_replays/' + file)
                else:
                    print(WARN + f'   File already exists in match_replays: {file}')
                    os.remove('cache/replay_buffer/' + file)
                break
        except PermissionError:
            pass

    # === Run r6-dissect on extracted replay ===
    # For folder in match_dir, run r6-dissect
    replay_jsons = []
    for folder in os.listdir('cache/replay_cache'):
        print(INFO + f'   Running r6-dissect on {folder}')
        replay_jsons.append(json.loads(subprocess.run(['./r6-dissect', 'cache/replay_cache/' + folder], capture_output=True).stdout.decode('utf-8')))

    # === Check for rehost ===
    # If the same map is played in two consecutive replays, rehost detected
    if any([replay_jsons[i]['rounds'][-1]['map']['name'] == replay_jsons[i + 1]['rounds'][-1]['map']['name'] for i in range(len(replay_jsons) - 1)]):
        team_1 = get_players_team(replay_jsons[0]['stats'][0]['username']).replace(' ', '_')
        team_2 = get_players_team(replay_jsons[0]['stats'][-1]['username']).replace(' ', '_')
        time_ = replay_jsons[0]['rounds'][0]['timestamp'].replace(':', '-')
        match_name = f'{team_1}-vs-{team_2}-{time_}'
        print(ERROR + f'   Rehost detected on {folder}. Moved to ./rehosted_replays')
        print(ACTION + f'   Resolution: Manually combine the replays in ./rehosted_replays/{folder}. Zip the resulting folder and move it to ./replay_buffer')
        os.mkdir('rehosted_replays/' + match_name)
        for folder in os.listdir('cache/replay_cache'):
            shutil.move('cache/replay_cache/' + folder, 'rehosted_replays/' + match_name + '/' + folder)
        os.remove('data/match_replays/' + file)
        return

    # === Generate stats dataframes from r6-dissect output ===
    # Player Stats
    print(INFO + '   Parsing player stats')
    for replay_json in replay_jsons:
        # If replay_json is empty, skip it
        if not replay_json:
            continue
        match_id, player_df = parse_json_player_stats(replay_json)
        player_df.to_csv(f'cache/write_cache/player_stats-{match_id}.csv', index=False)

    # Match Log
    print(INFO + '   Parsing match log')
    match_id, match_log_df = parse_json_match_log(replay_jsons)
    match_log_df.to_csv(f'cache/write_cache/match_log-{match_id}.csv', index=False)

    # === Empty replay_cache folder ===
    # Recursively delete all files, then delete all folders
    time_start = time()
    while True:
        if time() - time_start > 10:
            # Get list of all files and another list of all directories
            files = []
            dirs = []
            for root, dirnames, filenames in os.walk(os.path.join('cache', 'replay_cache')):
                for file in filenames:
                    files.append(os.path.join(root, file))
                for dir_ in dirnames:
                    dirs.append(os.path.join(root, dir_))

            # If files exist, require manual deletion
            if len(files):
                print(ERROR + 'Failed to empty replay_cache folder')
                print(ACTION + f'Resolution: Delete the following files manually: {", ".join(files)}. Press [ENTER] when action is completed')
                input()
            else:
                # If directories exist, warn that they should be deleted, but don't halt
                print(WARN + f'Failed to delete empty replay cache directory')
                print(ACTION + f'Resolution: Delete the following directories manually: {", ".join(dirs)}')
                break

        try:
            for root, dirs, files in os.walk(os.path.join('cache', 'replay_cache'), topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            break
        except PermissionError:
            pass


def parse_json_match_log(replay_jsons):
    match_log_df = pd.DataFrame(columns=[
        'Time',
        'Team',
        'Opponent',
        'Map 1',
        'Map 1 Score',
        'Map 1 Opp Score',
        'Map 1 Win',
        'Map 2',
        'Map 2 Score',
        'Map 2 Opp Score',
        'Map 2 Win',
        'Map 3',
        'Map 3 Score',
        'Map 3 Opp Score',
        'Map 3 Win',
        'Maps Won',
        'Maps Lost',
        'Win',
        'Round Diff',
        'Playoff?'
    ])

    # Get team names
    my_team = get_players_team(replay_jsons[0]['stats'][0]['username'])
    opponent_team = get_players_team(replay_jsons[0]['stats'][-1]['username'])

    # Get map names and scores
    maps = []
    for replay_json in replay_jsons:
        team_1_name = get_players_team(replay_json['rounds'][0]['players'][0]['username'])
        team_idx = replay_json['rounds'][0]['players'][0]['teamIndex'] if team_1_name == my_team else replay_json['rounds'][0]['players'][-1]['teamIndex']
        opponent_idx = 0 if team_idx == 1 else 1

        score_for = 0
        score_against = 0
        for round_ in replay_json['rounds']:
            if round_['teams'][team_idx]['score'] > score_for:
                score_for = round_['teams'][team_idx]['score']
            if round_['teams'][opponent_idx]['score'] > score_against:
                score_against = round_['teams'][opponent_idx]['score']

        map_ = {}
        map_['name'] = replay_json['rounds'][-1]['map']['name']
        time_string = replay_json['rounds'][0]['timestamp'].replace('T', ' ').replace('Z', '')
        map_['time'] = pd.to_datetime(time_string).timestamp()
        map_['score_for'] = score_for
        map_['score_against'] = score_against
        map_['win'] = True if map_['score_for'] > map_['score_against'] else False
        maps.append(map_)
    maps = sorted(maps, key=lambda x: x['time'])

    # If less than 3 maps, add empty maps
    while len(maps) < 3:
        maps.append({'name': '', 'time': '', 'score_for': 0, 'score_against': 0, 'win': ''})

    # Other stats
    time_ = datetime.fromtimestamp(maps[0]['time']).strftime('%m-%d-%Y %H:%M:%S')
    maps_won = len([map_ for map_ in maps if map_['win'] == True])
    maps_lost = len([map_ for map_ in maps if map_['win'] == False])
    win = True if maps_won > maps_lost else False
    round_diff = sum([map_['score_for'] - map_['score_against'] for map_ in maps])

    # Add to dataframe
    match_log_df = pd.concat([match_log_df, pd.DataFrame.from_records([{
        'Time': time_,
        'Team': my_team,
        'Opponent': opponent_team,
        'Map 1': maps[0]['name'],
        'Map 1 Score': maps[0]['score_for'],
        'Map 1 Opp Score': maps[0]['score_against'],
        'Map 1 Win': maps[0]['win'],
        'Map 2': maps[1]['name'],
        'Map 2 Score': maps[1]['score_for'],
        'Map 2 Opp Score': maps[1]['score_against'],
        'Map 2 Win': maps[1]['win'],
        'Map 3': maps[2]['name'],
        'Map 3 Score': maps[2]['score_for'],
        'Map 3 Opp Score': maps[2]['score_against'],
        'Map 3 Win': maps[2]['win'],
        'Maps Won': maps_won,
        'Maps Lost': maps_lost,
        'Win': win,
        'Round Diff': round_diff,
        'Playoff?': True if maps_won + maps_lost > 1 else False
    }])], ignore_index=True)

    # Add the same data to the other team
    match_log_df = pd.concat([match_log_df, pd.DataFrame.from_records([{
        'Time': time_,
        'Team': opponent_team,
        'Opponent': my_team,
        'Map 1': maps[0]['name'],
        'Map 1 Score': maps[0]['score_against'],
        'Map 1 Opp Score': maps[0]['score_for'],
        'Map 1 Win': not maps[0]['win'],
        'Map 2': maps[1]['name'],
        'Map 2 Score': maps[1]['score_against'],
        'Map 2 Opp Score': maps[1]['score_for'],
        'Map 2 Win': not maps[1]['win'] if type(maps[1]['win']) == bool else '',  # If map 2 is empty, don't show win/loss
        'Map 3': maps[2]['name'],
        'Map 3 Score': maps[2]['score_against'],
        'Map 3 Opp Score': maps[2]['score_for'],
        'Map 3 Win': not maps[2]['win'] if type(maps[2]['win']) == bool else '',
        'Maps Won': maps_lost,
        'Maps Lost': maps_won,
        'Win': not win,
        'Round Diff': -round_diff,
        'Playoff?': True if maps_won + maps_lost > 1 else False
    }])], ignore_index=True)

    match_id = replay_jsons[0]['rounds'][0]['recordingProfileID'] + str(replay_jsons[0]['rounds'][0]['additionalTags']) + replay_jsons[0]['rounds'][0]['timestamp'].replace('-', '').replace(':', '').replace('Z', '').replace('T', '')
    return match_id, match_log_df


def parse_json_player_stats(replay_json):
    player_df = pd.DataFrame(columns=['player', 'team', 'opponent', 'map', 'kills', 'deaths', 'assists', 'headshots', 'objectives', 'trades', 'opening kill', 'opening death', '2ks', '3ks', '4ks', 'aces', 'rounds', 'kost rounds', 'suicides', 'teamkills', '1vX'])

    # Stats from json stats section
    for player in replay_json['stats']:
        player_df = pd.concat([
            player_df,
            pd.DataFrame.from_records([{
                'player': player['username'],
                'rounds': player['rounds'],
                'kills': player['kills'],
                'deaths': player['deaths'],
                'assists': player['assists'],
                'headshots': player['headshots']
            }])
        ], ignore_index=True)

    # Player's teams
    player_df['team'] = player_df['player'].apply(get_players_team)
    player_df['opponent'] = list(player_df['team'])[::-1]

    # Map
    player_df['map'] = replay_json['rounds'][0]['map']['name']

    # Objective
    player_df['objectives'] = 0
    objective_log = []
    for round_num, round_ in enumerate(replay_json['rounds']):
        if round_['matchFeedback'] is None:  # Skip if no matchFeedback
            continue
        for event in round_['matchFeedback']:
            if event['type']['name'] == 'DefuserPlantComplete' or event['type']['name'] == 'DefuserDisableComplete' and (round_num, event['username']) not in objective_log:
                player = event['username']
                player_df.loc[player_df['player'] == player, 'objectives'] += 1
                objective_log.append((round_num, player))

    # Trades
    player_df['trades'] = 0
    kill_feed = []
    for round_ in replay_json['rounds']:
        round_kill_feed = []
        if round_['matchFeedback'] is None:  # Skip if no matchFeedback
            continue
        for event in round_['matchFeedback']:
            if event['type']['name'] == 'Kill':
                killer = event['username']
                killed = event['target']
                time = event['timeInSeconds']
                round_kill_feed.append((killer, killed, time))
        kill_feed.append(round_kill_feed)

    # Trade counts if someone kills someone who just got a kill within 3 seconds
    trade_log = []
    for round_num, round_kills in enumerate(kill_feed):
        for i in range(len(round_kills)):
            for j in range(i + 1, len(round_kills)):
                if round_kills[i][0] == round_kills[j][1] and abs(round_kills[j][2] - round_kills[i][2]) <= 3:
                    player_df.loc[player_df['player'] == round_kills[j][0], 'trades'] += 1
                    trade_log.append((round_num, round_kills[j][0]))

    # Opening kills/deaths
    player_df['opening kill'] = 0
    player_df['opening death'] = 0
    for round_kills in kill_feed:
        if len(round_kills):
            opening_kill = round_kills[0][0]
            opening_death = round_kills[0][1]
            player_df.loc[player_df['player'] == opening_kill, 'opening kill'] += 1
            player_df.loc[player_df['player'] == opening_death, 'opening death'] += 1

    # 2k, 3k, 4k, ace
    player_df['2ks'] = 0
    player_df['3ks'] = 0
    player_df['4ks'] = 0
    player_df['aces'] = 0
    for round_ in replay_json['rounds']:
        for player in round_['stats']:
            if player['kills'] == 2:
                player_df.loc[player_df['player'] == player['username'], '2ks'] += 1
            elif player['kills'] == 3:
                player_df.loc[player_df['player'] == player['username'], '3ks'] += 1
            elif player['kills'] == 4:
                player_df.loc[player_df['player'] == player['username'], '4ks'] += 1
            elif player['kills'] == 5:
                player_df.loc[player_df['player'] == player['username'], 'aces'] += 1

    # KOST rounds
    player_df['kost rounds'] = 0
    for round_num in range(len(replay_json['rounds'])):
        for player in player_df['player'].values:
            survived, got_kill, got_trade, did_objective = False, False, False, False
            for player_stat in replay_json['rounds'][round_num]['stats']:
                if player_stat['username'] == player:
                    survived = not player_stat['died']
                    got_kill = player_stat['kills'] > 0
            for trade in trade_log:
                if trade[0] == round_num and trade[1] == player:
                    got_trade = True
            if replay_json['rounds'][round_num]['matchFeedback'] is None:  # Skip if no matchFeedback
                continue
            for event in replay_json['rounds'][round_num]['matchFeedback']:
                if event['username'] == player and event['type']['name'] == 'DefuserPlantComplete':
                    did_objective = True
            if survived or got_kill or got_trade or did_objective:
                player_df.loc[player_df['player'] == player, 'kost rounds'] += 1

    # Suicides
    player_df['suicides'] = 0
    for round_ in replay_json['rounds']:
        if round_['matchFeedback'] is None:  # Skip if no matchFeedback
            continue
        for event in round_['matchFeedback']:
            if event['type']['name'] == 'Death':
                player_df.loc[player_df['player'] == event['username'], 'suicides'] += 1

    # Teamkills
    player_df['teamkills'] = 0
    for round_num, round_feed in enumerate(kill_feed):
        for kill in round_feed:
            killer, target = kill[0], kill[1]
            if get_players_team(killer) == get_players_team(target):
                player_df.loc[player_df['player'] == killer, 'teamkills'] += 1

    # 1vX clutches
    player_df['1vX'] = 0
    for round_ in replay_json['rounds']:
        team_names = player_df['team'].unique()
        team_1_players = list(player_df[player_df['team'] == team_names[0]]['player'].values)
        team_2_players = list(player_df[player_df['team'] == team_names[1]]['player'].values)

        for player in round_['stats']:
            if player['died']:
                if player['username'] in team_1_players:
                    team_1_players.remove(player['username'])
                elif player['username'] in team_2_players:
                    team_2_players.remove(player['username'])

        # If only one left on your team and your team won the round
        if len(team_1_players) == 1:
            team_index = 0
            for player in round_['players']:
                if player['username'] == team_1_players[0]:
                    team_index = player['teamIndex']

            if round_['teams'][team_index]['won']:
                player_df.loc[player_df['player'] == team_1_players[0], '1vX'] += 1
        elif len(team_2_players) == 1:
            team_index = 0
            for player in round_['players']:
                if player['username'] == team_2_players[0]:
                    team_index = player['teamIndex']

            if round_['teams'][team_index]['won']:
                player_df.loc[player_df['player'] == team_2_players[0], '1vX'] += 1

    match_id = replay_json['rounds'][0]['recordingProfileID'] + str(replay_json['rounds'][0]['additionalTags']) + replay_json['rounds'][0]['timestamp'].replace('-', '').replace(':', '').replace('Z', '').replace('T', '')

    return match_id, player_df


def main():
    auth()
    while True:
        if os.path.exists('cache/replay_buffer'):
            files = os.listdir('cache/replay_buffer')
            for file in files:
                parse_file(file)
        sleep(1)

if __name__ == '__main__':
    main()
