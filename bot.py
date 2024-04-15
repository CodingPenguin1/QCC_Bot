import os
import discord
from dotenv import load_dotenv
import pandas as pd
from tabulate import tabulate


load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)


@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')


async def g_stats(message):
    if message.content.startswith('teams'):
        df = pd.read_csv('data/player_stats.csv')
        teams = sorted(list(df['Team'].unique()))
        await message.reply(f'Teams:\n- {"\n- ".join(teams)}')

    elif message.content.startswith('stats'):
        df = pd.read_csv('data/player_stats.csv')

        # Team specified, so filter by team
        if len(message.content.split(' ')) > 2:
            team = ' '.join(message.content.split(' ')[1:])
            df = df[df['Team'] == team]
            df = df[['Player', 'K/D', 'KOST', 'SRV', 'Rating', 'Headshot %', 'Entry', 'KPR']]

        # No team specified, so show all teams
        else:
            df = df[['Team', 'Player', 'K/D', 'KOST', 'SRV', 'Rating', 'Headshot %', 'Entry', 'KPR']]

        # Round K/D, KOST, SRV, Rating, KPR to 2 decimal places
        df['K/D'] = df['K/D'].round(2)
        df['KOST'] = df['KOST'].round(2)
        df['SRV'] = df['SRV'].round(2)
        df['Rating'] = df['Rating'].round(2)
        df['KPR'] = df['KPR'].round(2)

        # Change Headshot % to percentage
        df['Headshot %'] = (df['Headshot %'] * 100).round()

        # Tabulate
        table = tabulate(df, headers='keys', tablefmt='fancy_grid', showindex=False)

        # If table string > 2000 characters, split into multiple messages
        if len(table) > 2000:
            lines = table.split('\n')

            start_index = 0
            char_count = 0
            for i in range(len(lines)):
                char_count += len(lines[i])
                if char_count > 1900:
                    await message.reply(f'```{'\n'.join(lines[start_index:i])}```')
                    char_count = 0
                    start_index = i
            await message.reply(f'```{'\n'.join(lines[start_index:])}```')
        else:
            await message.reply(f'**{team}**\n```{table}```')


@client.event
async def on_message(message):
    if message.content.startswith('teams') or message.content.startswith('stats'):
        await g_stats(message)

    # If message sent in #match-report and isn't from the bot
    if message.channel.id == 1228175751648509973 and message.author.id != 1223654836189397002:
        for file in message.attachments:
            if not file.filename.endswith('.zip'):
                return

            # Save the file to cache/replay_buffer
            await file.save(os.path.join('cache', 'replay_buffer', file.filename))
            await message.reply(f'{file.filename} submitted successfully!')


def main():
    client.run(TOKEN)


if __name__ == '__main__':
    main()
