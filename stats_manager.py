import gspread
from gspread.cell import Cell
from oauth2client.service_account import ServiceAccountCredentials
import os
import pandas as pd
from time import sleep
import numpy as np
import warnings
from colorama import Fore

INFO = f'{Fore.GREEN}[INF]{Fore.RESET} '
WARN = f'{Fore.YELLOW}[WRN]{Fore.RESET} '
ERROR = f'{Fore.RED}[ERR]{Fore.RESET} '
ACTION = f'{Fore.CYAN}[ACT]{Fore.RESET} '

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=RuntimeWarning)

client = None
roster_list = None
# Using https://medium.com/daily-python/python-script-to-edit-google-sheets-daily-python-7-aadce27846c0


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


def update_player_chart_stats():
    print(INFO + '   Updating player chart stats')

    # Load stats csv and sheet
    df = pd.read_csv('data/player_stats.csv')
    sheet = client.open('QCC 2024 Stats').worksheet('!Chart Data')

    # Top 10 players by K/D (A2:B11) columns are name, kd
    kd_df = df.sort_values(by='K/D', ascending=False).head(10)
    cells = []
    i = 0
    for _, row in kd_df.iterrows():
        cells.append(Cell(row=i + 2, col=1, value=row['Player']))
        cells.append(Cell(row=i + 2, col=2, value=row['K/D']))
        i += 1
    sheet.update_cells(cells)

    # Top 10 players by KOST (D2:E11) columns are name, kost
    print(INFO + 'Updating player chart stats')
    kost_df = df.sort_values(by='KOST', ascending=False).head(10)
    cells = []
    i = 0
    for _, row in kost_df.iterrows():
        cells.append(Cell(row=i + 2, col=4, value=row['Player']))
        cells.append(Cell(row=i + 2, col=5, value=row['KOST']))
        i += 1
    sheet.update_cells(cells)

    # TODO: best performances on a single map


def update_bracket():
    def get_team_group(team):
        global roster_list
        if roster_list is None:
            sheet = client.open('QCC 2024 Stats').worksheet('!Roster List')
            data = sheet.get_all_values()
            roster_list = pd.DataFrame(data[1:], columns=data[0])

        team_row = roster_list[roster_list['Team'] == team]
        group = team_row['Group'].values[0]
        return group

    print(INFO + '   Updating standings')
    sheet = client.open('QCC 2024 Stats').worksheet('!Standings')
    data = sheet.get_all_values()

    # Get the team names from A2:A
    teams = [row[0] for row in data[1:]]
    while '' in teams:
        teams.remove('')
    teams = np.array(teams)

    # Get points, wins-losses, and round differentials from D2:D E2:E and F2:F
    points = np.array([row[3] for row in data[1:len(teams) + 1]])
    wins_losses = np.array([row[4] for row in data[1:len(teams) + 1]])
    round_diffs = np.array([row[5] for row in data[1:len(teams) + 1]])

    # Sort by win-loss, then round differential
    sorted_teams = list(teams[np.lexsort((round_diffs, wins_losses, points))])[::-1]

    # Split into groups
    sorted_groups = [[], [], [], []]
    for i in range(len(sorted_teams)):
        group = get_team_group(sorted_teams[i])
        sorted_groups[int(group) - 1].append(sorted_teams[i])

    # Write to standings sheet L2:O
    print(INFO + '   Writing to standings sheet')
    cells = []
    for i in range(4):
        for j in range(4):
            cells.append(Cell(row=j + 2, col=i + 12, value=sorted_groups[i][j] if j < len(sorted_groups[i]) else ''))
    sheet.batch_clear(['L2:O'])  # Clear all rows except header
    sheet.update_cells(cells)


def write_player_stats(file):
    print(INFO + f'Processing player stats from {file}')
    # If raw player stats doesn't exist, create it from update file
    if not os.path.exists('data/raw_player_stats.csv'):
        df = pd.read_csv('cache/write_cache/' + file)
        df.to_csv('data/raw_player_stats.csv', index=False)
    # Otherwise, concatentate the two
    else:
        df = pd.read_csv('cache/write_cache/' + file)
        raw_df = pd.read_csv('data/raw_player_stats.csv')
        df = pd.concat([df, raw_df])
        df.to_csv('data/raw_player_stats.csv', index=False)

    # Create sheet for processed player stats
    processed_df = pd.DataFrame(columns=[
        'Team',
        'Player',
        'KOST',
        'K',
        'D',
        'A',
        'Rounds',
        'K/D',
        'KPR',
        'SRV',
        'A/D',
        'APR',
        'OBJ',
        'Trade',
        'Entry',
        '1 v Xs',
        'Headshot %',
        '2Ks',
        '3Ks',
        '4Ks',
        'Aces',
        'Suicides',
        'Teamkills',
        'Rating',
    ])

    players = df['player'].unique()
    for player in players:
        player_dict = {}
        # Raw Stats
        player_dict['Player'] = player
        player_dict['Team'] = df[df['player'] == player]['team'].values[0]
        player_dict['K'] = df[df['player'] == player]['kills'].sum()
        player_dict['D'] = df[df['player'] == player]['deaths'].sum()
        player_dict['A'] = df[df['player'] == player]['assists'].sum()
        player_dict['Rounds'] = df[df['player'] == player]['rounds'].sum()
        player_dict['OBJ'] = df[df['player'] == player]['objectives'].sum()
        player_dict['Trade'] = df[df['player'] == player]['trades'].sum()
        player_dict['2Ks'] = df[df['player'] == player]['2ks'].sum()
        player_dict['3Ks'] = df[df['player'] == player]['3ks'].sum()
        player_dict['4Ks'] = df[df['player'] == player]['4ks'].sum()
        player_dict['Aces'] = df[df['player'] == player]['aces'].sum()
        player_dict['Suicides'] = df[df['player'] == player]['suicides'].sum()
        player_dict['Teamkills'] = df[df['player'] == player]['teamkills'].sum()
        player_dict['1 v Xs'] = df[df['player'] == player]['1vX'].sum()

        # Derived Stats
        player_dict['KOST'] = df[df['player'] == player]['kost rounds'].sum() / player_dict['Rounds']
        player_dict['K/D'] = player_dict['K'] / player_dict['D']
        if player_dict['K/D'] == np.inf:
            player_dict['K/D'] = player_dict['K']
        player_dict['KPR'] = player_dict['K'] / player_dict['Rounds']
        player_dict['SRV'] = 1 - (player_dict['D'] / player_dict['Rounds'])
        player_dict['A/D'] = player_dict['A'] / player_dict['D']
        if player_dict['A/D'] == np.inf:
            player_dict['A/D'] = player_dict['A']
        player_dict['APR'] = player_dict['A'] / player_dict['Rounds']
        player_dict['Entry'] = df[df['player'] == player]['opening kill'].sum() - df[df['player'] == player]['opening death'].sum()
        player_dict['Headshot %'] = df[df['player'] == player]['headshots'].sum() / player_dict['K']
        if player_dict['Headshot %'] == np.inf:
            player_dict['Headshot %'] = df[df['player'] == player]['headshots'].sum()
        player_dict['Rating'] = 0.7937*player_dict['KPR'] + 0.9091*player_dict['APR'] + 0.9375*player_dict['SRV']

        processed_df = pd.concat([processed_df, pd.DataFrame(player_dict, index=[len(processed_df) + 1])])

    # Replace NaN with 0
    processed_df = processed_df.fillna(0)
    # Sort by team and player, inverted so we can insert at the top of the sheet
    processed_df = processed_df.sort_values(by=['Team', 'Player'], ascending=[False, False])

    # Create cell objects
    cells = []
    for row_i, row in processed_df.iterrows():
        for col_i, cell in enumerate(row):
            cells.append(Cell(row=row_i + 1, col=col_i + 1, value=cell))

    # Write new cell data to sheet
    print(INFO + '   Writing to player stats sheet')
    sheet = client.open('QCC 2024 Stats').worksheet('!Player Stats')
    sheet.batch_clear(['A2:X'])  # Clear all rows except header
    sheet.update_cells(cells)

    # Write to data folder
    processed_df.to_csv('data/player_stats.csv', index=False)

    # Update chart stats
    # update_player_chart_stats()


def write_match_log(file):
    print(INFO + f'   Writing {file} to sheet match log')

    # Load stats csv and sheet
    df = pd.read_csv('cache/write_cache/' + file)
    sheet = client.open('QCC 2024 Stats').worksheet('!Match Log')

    # If new rows in match log, add to saved match log
    if os.path.exists('data/match_log.csv'):
        match_log = pd.read_csv('data/match_log.csv')
        for i, row in df.iterrows():
            for j, match_row in match_log.iterrows():
                if row.equals(match_row):
                    team_1 = df['Team'].values[0]
                    team_2 = df['Opponent'].values[0]
                    time = df['Time'].values[0]
                    print(WARN + f'   Duplicate match - match {team_1} vs {team_2} at {time} already exists in match log')
                    return False
        # Add to saved match log
        df = pd.concat([df, match_log])
    df.to_csv('data/match_log.csv', index=False)

    # Create cell objects
    print(INFO + '   Writing match log to sheet')
    df = pd.read_csv('data/match_log.csv')
    cells = []
    for i, row in df.iterrows():
        for j, cell in enumerate(row):
            if not pd.isnull(cell):
                cells.append(Cell(row=i + 2, col=j + 1, value=cell))

    # Write new cell data to sheet
    sheet.batch_clear(['A2:T'])  # Clear all rows except header
    sheet.update_cells(cells)

    #update_bracket()
    update_map_stats()
    return True


def update_map_stats():
    print(INFO + '   Updating map stats')
    map_stats_df = pd.DataFrame(columns=[
        'Team',
        'Map',
        'Rounds Won',
        'Rounds Lost',
        'Wins',
        'Losses',
        'Round Differential',
        'Win %',
    ])

    # Load match log
    df = pd.read_csv('data/match_log.csv')

    # Get all teams and maps
    filter_sheet = client.open('QCC 2024 Stats').worksheet('!Filters')
    data = filter_sheet.get_all_values()
    teams, maps = [], []
    for row in data[1:]:
        teams.append(row[0])
        maps.append(row[3])
    while '' in teams:
        teams.remove('')
    while '' in maps:
        maps.remove('')

    # Map for dissect map names to actual names
    map_names_mapping = {}
    for i in range(len(maps)):
        map_names_mapping[maps[i]] = data[i+1][2]

    # Make 0s dataframe
    for team in teams:
        for map_ in maps:
            map_stats_df = pd.concat([map_stats_df, pd.DataFrame({
                'Team': team,
                'Map': map_names_mapping[map_],
                'Rounds Won': 0,
                'Rounds Lost': 0,
                'Wins': 0,
                'Losses': 0,
                'Round Differential': 0,
                'Win %': 0
            }, index=[len(map_stats_df) + 1])])

    # Iterate through all teams and maps
    for team in teams:
        for map_ in maps:
            readable_name = map_names_mapping[map_]
            rounds_won, rounds_lost, wins, losses = 0, 0, 0, 0
            for _, row in df.iterrows():
                if row['Team'] == team:
                    for i in range(3):
                        if type(row[f'Map {i + 1}']) == str:
                            if row[f'Map {i + 1}'] == map_:
                                rounds_won += row[f'Map {1 + i} Score']
                                rounds_lost += row[f'Map {1 + i} Opp Score']
                                wins += 1 if row[f'Map {1 + i} Win'] else 0
                                losses += 1 if not row[f'Map {1 + i} Win'] else 0

                try:
                    win_percent = wins / (wins + losses)
                except ZeroDivisionError:
                    win_percent = 0
                # Add to map_stats_df
                map_stats_df.loc[(map_stats_df['Team'] == team) & (map_stats_df['Map'] == readable_name), 'Rounds Won'] = rounds_won
                map_stats_df.loc[(map_stats_df['Team'] == team) & (map_stats_df['Map'] == readable_name), 'Rounds Lost'] = rounds_lost
                map_stats_df.loc[(map_stats_df['Team'] == team) & (map_stats_df['Map'] == readable_name), 'Wins'] = wins
                map_stats_df.loc[(map_stats_df['Team'] == team) & (map_stats_df['Map'] == readable_name), 'Losses'] = losses
                map_stats_df.loc[(map_stats_df['Team'] == team) & (map_stats_df['Map'] == readable_name), 'Round Differential'] = rounds_won - rounds_lost
                map_stats_df.loc[(map_stats_df['Team'] == team) & (map_stats_df['Map'] == readable_name), 'Win %'] = win_percent

    # Write to sheet (A2:F)
    print(INFO + '   Writing to map stats sheet')
    sheet = client.open('QCC 2024 Stats').worksheet('!Map Stats')
    cells = []
    for i, row in map_stats_df.iterrows():
        for j, cell in enumerate(row):
            cells.append(Cell(row=i + 1, col=j + 1, value=cell))
    sheet.batch_clear(['A2:H'])  # Clear all rows except header
    sheet.update_cells(cells)


def write_data(file):
    while True:
        try:
            sleep(10)
            with open('cache/write_cache/' + file, 'r') as f:
                _ = f.read()
            break
        except PermissionError:
            pass

    if file.startswith('match_log'):
        print(INFO + f'Processing match {file.replace("match_log-", "").replace(".csv", "")}')
        duplicate_match = not write_match_log(file)
        print(INFO + f'   Clearing {file} from write cache')
        os.remove('cache/write_cache/' + file)
    elif file.startswith('player_stats'):
        write_player_stats(file)
        print(INFO + f'   Clearing {file} from write cache')
        os.remove('cache/write_cache/' + file)


def main():
    auth()

    while True:
        # If file in 'write_cache' directory
        if os.path.exists('cache/write_cache'):
            files = os.listdir('cache/write_cache')
            for file in files:
                write_data(file)


if __name__ == '__main__':
    main()
