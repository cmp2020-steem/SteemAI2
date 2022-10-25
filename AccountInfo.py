from steem import Steem
from steem.account import Account
from datetime import datetime, timedelta
from steembase.exceptions import RPCError
from time import sleep, time
import requests
import re
from Downloading.SteemSQL import SSQL
from pycoingecko import CoinGeckoAPI
from steem.converter import Converter
from timeFunctions import convertTime, convertTimeStamp, getTime
import asyncio
import pandas as pd

converter = Converter()
DB = SSQL()

s = Steem()
cg = CoinGeckoAPI()

def get_block(date):
    now = datetime.now()
    difference = now - date
    current = requests.get('https://sds.steemworld.org/blocks_api/getLastIrreversibleBlockNum').json()['result']
    return int(current - (difference.days * 28800) - (difference.seconds * 1/3))

def get_block_range(start, end):
    found = False
    first = start
    if end > start + 249:
        last = first + 249
    else:
        last = end
    while not found and first < last and last <= end:
        response = requests.get(f'https://sds.steemworld.org/blocks_api/getVirtualOpsInBlockRange/{first}-{last}')
        rows = response.json()['result'][0]
        for block in rows:
            if block[0] == 'fill_vesting_withdraw':
                info = block[1]
                if 'STEEM' in info['deposited']:
                    vests = float(re.findall("\d+.\d+", f"{info['withdrawn']}")[0])
                    steem = float(re.findall("\d+.\d+", f"{info['deposited']}")[0])

                    if steem > 2:
                        return vests/steem

        first = last + 1
        if first + 249 < end:
            last = first + 249
        else:
            last = end

def getPrice(coin, date):
    history = cg.get_coin_history_by_id(id=coin, date=date)
    price = history["market_data"]["current_price"]["usd"]
    return price

def get_closest_index(acct, article_block: int):
    account = Account(acct)
    while True:
        try:
            history = account.get_account_history(-1, 0)
            break
        except RPCError as e:
            print(e)
            sleep(1)
    for h in history:
        upper = h['index']
    lower = 0
    while lower < upper - 1:
        index = (upper + lower) // 2
        while True:
            try:
                history = account.get_account_history(index, 0)
                break
            except RPCError as e:
                print(e)
                sleep(1)
        for h in history:
            block = h['block']
        if block > article_block:
            upper = index
            lower = lower
        elif block < article_block:
            upper = upper
            lower = index
        else:
            return index
    return index

async def get_author_followers(post):
    current_followers = len(requests.get(f'https://sds.steemworld.org/followers_api/getFollowers/{post["author"]}').json()['result'])
    timestamp = getTime(convertTime(post['created']) + timedelta(minutes=60))
    added_followers = len(requests.get(f'https://sds.steemworld.org/followers_api/getFollowedHistory/{post["author"]}/{timestamp}-{int(time())}').json()['result']['rows'])
    return current_followers - added_followers

async def get_author_rewards(post:str, n:int = 30):
    timestamp = convertTime(post['created'])
    start = getTime(timestamp - timedelta(n))
    end = getTime(timestamp)
    response = requests.get(f'https://sds.steemworld.org/rewards_api/getRewards/author_reward/{post["author"]}/{start}-{end}')
    rows = response.json()['result']['rows']
    total = 0
    count = 0
    for reward in rows:
        total += reward[5] #index 5 is vests
        count += 1
    if count != 0:
        return (total // count, count)
    else:
        return (0, 0)

async def get_curator_rewards_dollars(curator:str, time:str, n:int=30):
    if curator == None:
        return 0.0
    timestamp = convertTime(time)
    start = getTime(timestamp - timedelta(n))
    end = getTime(timestamp)
    response = requests.get(f'https://sds.steemworld.org/rewards_api/getRewards/curation_reward/{curator}/{start}-{end}/10000')
    rows = response.json()['result']['rows']
    total_vests = 0
    total_rewards = 0
    for r in rows:
        total_vests += r[1]
        total_rewards += 1
    if total_rewards != 0:
        return total_vests // total_rewards
    else:
        return 0.0

def get_curator_rewards_vests(curator:str, time:str, n:int=30):
    prices = DB.get_data('historic_steem_prices')
    timestamp = convertTime(time)
    start = getTime(timestamp - timedelta(n))
    end = getTime(timestamp)
    response = requests.get(
        f'https://sds.steemworld.org/rewards_api/getRewards/curation_reward/{curator}/{start}-{end}/10000')
    rows = response.json()['result']['rows']
    factor = converter.vests_to_sp(1000) / 1000
    total_vests = 0
    total_rewards = 0
    for r in rows:
        total_vests += r[1]
        total_rewards += 1
    if total_rewards != 0:
        return total_vests / total_rewards
    else:
        return 0.0

'''def get_author_reward_history(author:str, article_block:int, timestamp:str, n:int=30):
    index = get_closest_index(acct=author, article_block=article_block)
    account = Account(author)
    maxdate = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S') - timedelta(days=7)
    mindate = maxdate - timedelta(days=n)
    total_rewards = 0
    posts = 0
    lv = False
    count = 0
    while not lv:
        count += 1
        if index > 100:
            limit = 100
        elif index > 0:
            limit = index - 1
        else:
            break
        while True:
            try:
                history = account.get_account_history(index=index, limit=limit)
                break
            except RPCError as e:
                print(e)
                sleep(1)
        for h in history:
            time = datetime.strptime(h['timestamp'], '%Y-%m-%dT%H:%M:%S')
            index = h['index'] - 1
            if mindate <= time <= maxdate:
                if 'parent_author' in h.keys():
                    if h['parent_author'] == '':

                        author = h['author']
                        permlink = h['permlink']
                        article = s.get_content(author, permlink)
                        total_rewards += 2 * float(article['curator_payout_value'].replace(' STEEM', '').replace(' SBD', ''))
                        posts += 1
            elif time < mindate:
                lv = True
                break
            else:
                if count > 100:
                    break
        if index > 100:
            index -= 100
        elif index > 0:
            index -= 1
        else:
            break
            lv = False
    if posts != 0:
        average = float(total_rewards/posts)
    else:
        average = 0.00
    return average, posts

def get_curator_reward_history(curator:str, article_block:int, timestamp:str, n:int=7):
    if curator == None:
        return 0
    index = get_closest_index(acct=curator, article_block=article_block)
    account = Account(curator)
    maxdate = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S') - timedelta(days=7)
    mindate = maxdate - timedelta(days=n)
    total_rewards = 0
    posts = 0
    lv = False
    count = 0
    while not lv:
        if index > 100:
            limit = 100
        else:
            limit = index - 1
        while True:
            try:
                history = account.get_account_history(index=index, limit=limit)
                break
            except RPCError as e:
                print(e)
                sleep(1)
        for h in history:
            time = datetime.strptime(h['timestamp'], '%Y-%m-%dT%H:%M:%S')
            index = h['index'] - 1
            if mindate <= time <= maxdate:
                if 'curator' in h.keys():
                    total_rewards += 2 * float(h['reward'].replace(' VESTS', ''))
                    posts += 1
            elif time < mindate:
                lv = True
                break
            else:
                if count > 100:
                    break
        if index > 100:
            index -= 100
        elif index > 0:
            index -= 1
        else:
            break
    if posts != 0:
        average = float(total_rewards / posts)
    else:
        average = 0.00
    return average'''

