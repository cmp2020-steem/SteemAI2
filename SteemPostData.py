from steem import Steem
from datetime import datetime, timedelta
from SteemConversions import get_displayed_reputation
from re import findall
import BodyAndLanguage as bl
from json import loads
from Downloading.SteemSQL import SSQL
from Downloading import AccountInfo as acc
import asyncio
import timeFunctions as tf
import requests

DB = SSQL()

s = Steem()

def check_post(post:dict) -> bool:
    body = bl.get_body(post)
    format_string = '%Y-%m-%dT%H:%M:%S'
    original_created = post['created']
    created = datetime.strptime(original_created, format_string)
    most = datetime.now() - timedelta(days=7)
    base = datetime.strptime("2019-08-27T23:59:59", format_string)
    if created >= base:
        if len(body) > 200 and len(body) < 3000:
            '''hour = get_post_historic_value(post, 60)
            if hour >= 0.05 and hour <= 2:
                return True'''
            return True
    return False

def get_post_data(post, block, df, articles):
    found = False
    if len(articles) > 0:
        for x in range(len(articles)):
            article = articles[x]
            if int(post['id']) == int(article['id']):
                found = True
                break
    if int(post['id'] in df['id'].values):
        df_post = df[df['id'] == int(post['id'])]
        author_avg_payout_1_week, author_total_posts_1_week = df_post['author_avg_payout_1_week'].values[0], df_post['author_total_posts_1_week'].values[0]

        median_rewards = df_post['median_voter_1_hour_avg_curation_rewards'].values[0]

        top_rewards = df_post['top_voter_1_hour_avg_curation_rewards'].values[0]

        resteems = df_post['resteems'].values[0]
        followers = df_post['followers'].values[0]
    elif found:
        author_avg_payout_1_week, author_total_posts_1_week = article['author_avg_payout_1_week'], article['author_total_posts_1_week']

        median_rewards = article['median_voter_1_hour_avg_curation_rewards']

        top_rewards = article['top_voter_1_hour_avg_curation_rewards']

        resteems = article['resteems'].values[0]
        followers = article['followers'].values[0]
    else:
        author_avg_payout_1_week, author_total_posts_1_week = asyncio.run(acc.get_author_rewards(post, 7))

        median_voter = get_post_historic_median_voter(post, 60)
        median_rewards = asyncio.run(acc.get_curator_rewards_dollars(median_voter, post['created'], 7))

        top_voter = get_post_historic_top_voter(post, 60)
        top_rewards = asyncio.run(acc.get_curator_rewards_dollars(top_voter, post['created'], 7))

        resteems = asyncio.run(get_post_historic_resteems(post, 60))
        followers = asyncio.run(acc.get_author_followers(post))

    body = bl.get_body(post)
    sentences = bl.get_sentences(post)

    primary_language, spelling_errors = asyncio.run(bl.get_language_and_spelling_errors(sentences))



    filtered = bl.get_filtered(post)

    body_word_count = len(body)
    body_paragraph_count = bl.get_paragraph_count(filtered)
    img_count = get_post_img_count(post)
    if img_count != 0:
        words_per_img = body_word_count // img_count
    else:
        words_per_img = 0

    steem_word_count = bl.get_occurence_of_steem_words(body)
    crypto_word_count = bl.get_occurence_of_crypto_words(body)

    edited = get_post_edited(post)

    tags = get_post_tags(post)

    #print(author_avg_payout_1_month)

    total_value = get_post_historic_value(post, 10080)
    author_value = get_post_author_value(post)
    curation_value = get_post_curation_value(post)
    beneficiary_value = get_post_beneficiary_value(curation_value, author_value)

    five_min_value = get_post_historic_value(post, 5)
    hour_value = get_post_historic_value(post, 60)
    value_change = get_change(hour_value, five_min_value)

    five_min_votes = get_post_historic_vote_count(post, 5)
    hour_votes = get_post_historic_vote_count(post, 60)
    votes_change = int(get_change(hour_votes, five_min_votes))

    data = {'id':int(post['id']), 'block':block, 'title':get_post_title(post), 'link':post['permlink'],
        'title_word_count':get_post_title_word_count(post), 'body_word_count':body_word_count, 'avg_sentence_length':bl.get_avg_sentence_length(sentences),
        'body_paragraph_count':body_paragraph_count, 'avg_paragraph_length':get_post_avg_paragraph_length(body_word_count, body_paragraph_count),
        'img_count':get_post_img_count(post), 'words_per_img':words_per_img, 'sentence_length_variance':bl.get_avg_sentence_length(sentences),
        'primary_language':primary_language, 'spelling_errors':spelling_errors, 'spelling_percent':bl.get_spelling_error_percent(spelling_errors, body_word_count),
        'spelling_errors_per_paragraph':get_words_per_paragraph(spelling_errors, body_paragraph_count), 'spelling_errors_per_sentence':get_words_per_paragraph(spelling_errors, len(sentences)),
        'created':post['created'], 'day':get_post_day(post), 'hour':get_post_hour(post), 'edited':int(edited),
        'tag1':tags[0], 'tag2':tags[1], 'tag3':tags[2], 'tag4':tags[3], 'tag5':tags[4], 'number_of_tags':len(tags), 'author':str(post['author']),
        'author_reputation':get_displayed_reputation(int(post['author_reputation'])),
        'author_avg_payout_1_week':author_avg_payout_1_week, 'author_total_posts_1_week':author_total_posts_1_week, 'total_beneficiaries':len(post['beneficiaries']),
        'total_votes':len(post['active_votes']), 'percent_sbd':int(post["percent_steem_dollars"] / 100), 'percent_burned':get_post_burn(post), 'total_value':total_value,
        'author_value':author_value, 'curation_value':curation_value, 'beneficiary_value':beneficiary_value, 'median_voter_1_hour_avg_curation_rewards':median_rewards,
        'top_voter_1_hour_avg_curation_rewards':top_rewards, '5_min_value':get_post_historic_value(post, 5), '1_hour_value':get_post_historic_value(post, 60),
        'resteems':resteems, '5_min_value':five_min_value, '1_hour_value':hour_value, 'value_change':value_change, '5_min_votes':five_min_votes,
        '1_hour_votes':hour_votes, 'votes_change':votes_change, 'followers':followers}
    return data

#7032

async def get_post_historic_resteems(post, n):
    created = tf.convertTime(post['created'])
    timestamp = tf.getTime(created + timedelta(minutes=n))
    response = requests.get(f'https://sds.steemworld.org/post_resteems_api/getResteems/{post["author"]}/{post["permlink"]}')
    rows = response.json()['result']['rows']
    resteems = 0
    for row in rows:
        if row[0] <= timestamp:
            resteems+=1
    return resteems

def get_post_title(post) -> str:
    """Returns Post Title"""
    title = str(post['title'])
    title = title.replace("'", "''")
    return title

def get_post_title_word_count(post) -> int:
    word_count = len(findall(" ", post['title']))
    return word_count

def get_post_avg_paragraph_length(body_word_count, body_paragraph_count):
    if body_word_count != 0:
        avg_paragraph_length = body_word_count // body_paragraph_count #NEW
    else:
        avg_paragraph_length = 0
    return avg_paragraph_length

def get_post_img_count(post) -> int:
    try:
        json_meta = loads(post['json_metadata'])
    except:
        return 0
    if 'image' in json_meta:
        return len(json_meta['image'])
    else:
        return 0

def get_percent_words(word_count, body_word_count):
    if word_count != 0:
        percent_words = int(body_word_count / word_count) * 100
    else:
        percent_words = 0
    return percent_words

def get_words_per_paragraph(word_count, paragraph_count):
    if word_count != 0:
        words_per_paragraph = int(paragraph_count // word_count)
    else:
        words_per_paragraph = 0
    return words_per_paragraph

def get_post_day(post) -> int:
    """Returns the day of the week the post was posted on"""
    date = datetime.strptime(post['created'], '%Y-%m-%dT%H:%M:%S')
    return date.weekday()

def get_post_hour(post) -> int:
    """Returns the hour the post was posted"""
    date = datetime.strptime(post['created'], '%Y-%m-%dT%H:%M:%S')
    return date.hour

def get_post_edited(post):
    lastupdate = format_time(post['last_update'])
    created = format_time(post['created'])
    return lastupdate != created

def get_post_tags(post):
    try:
        json_meta = loads(post['json_metadata'])
        if 'tags' in json_meta and len(json_meta['tags']) > 0:
            tags = []
            for tag in json_meta['tags']:
                if len(tag) <= 24 and len(tags) < 5:
                    tags.append(tag)
        else:
            tags = []
            category = post['category']
            if len(category) <= 24:
                tags.append(category)
    except ValueError:
        tags = []
        category = post['category']
        if len(category) <= 24:
            tags.append(category)
    while len(tags) < 5:
        tags.append(None)
    return tags

def get_post_curation_value(post) -> float:
    """Returns the post's curation value"""
    curation_value = findall("\d+.\d+", f"{post['curator_payout_value']}")
    return float("{:.2}".format(curation_value[0]))

def get_post_author_value(post) -> float:
    """Returns the post's author value"""
    author_value = findall("\d+.\d+", f"{post['total_payout_value']}")
    return float("{:.2}".format(author_value[0]))

def get_post_beneficiary_value(curation_value, author_value) -> float:
    """Returns the post's beneficiary value"""
    beneficiary_value = curation_value - author_value
    if beneficiary_value < 0:
        beneficiary_value = 0
    return beneficiary_value

def get_post_burn(post):
    for object in post['beneficiaries']:
        if object['account'] == 'null':
            return object['weight'] // 100
    return 0

def get_change(numerator, denominator):
    if denominator != 0:
        change = numerator / denominator
    else:
        change = numerator
    return change

#comments = s.get_content_replies(author=author, permlink=permlink)

def get_number(string:str) -> int:
    number = findall("\d+.\d+", f"{string}")
    return float("{:.2}".format(number[0]))

def format_time(time:str):
    format_string = '%Y-%m-%dT%H:%M:%S'
    date_string = time
    return datetime.strptime(date_string, format_string)

def track_vote_time(vote:dict, minutes: int, created:str) -> bool:
    time_created = format_time(created)
    time_change = timedelta(minutes=minutes)
    time_limit = time_created + time_change
    vote_time = format_time(vote['time'])
    return vote_time <= time_limit

def get_post_historic_value(post:dict, minutes:int) -> float:
    def get_voter_value(post:dict, active_votes: list, voter: str) -> float:
        total_value = get_number(post['total_payout_value']) + get_number(post['curator_payout_value'])
        total_rshares = get_post_historic_rshares(post, 10080)
        voter_rshares = 0
        for vote in active_votes:
            if vote['voter'] == voter:
                voter_rshares = int(vote['rshares'])
                break
        if total_rshares != 0:
            voter_value = (voter_rshares / total_rshares) * total_value
        else:
            voter_value = 0
        return float("{:.2f}".format(voter_value))

    historic_value = 0
    for vote in post['active_votes']:
        if track_vote_time(vote=vote, minutes=minutes, created=post['created']):
            voter = vote['voter']
            voter_value = get_voter_value(voter=voter, active_votes=post['active_votes'], post=post)
            historic_value += voter_value
        else:
            break
    historic_value = float("{:.2f}".format(historic_value))
    if historic_value < 0:
        historic_value = 0.00
    return historic_value

def get_post_historic_vote_count(post:dict, minutes:int) -> int: #Votes at any specified point
    """Returns total votes at different points in the post's history

    minutes: int representing the historical point to track votes"""
    vote_count = 0
    for vote in post['active_votes']:
        if track_vote_time(vote=vote, minutes=minutes, created=post['created']):
            vote_count += 1
        else:
            break
    return int(vote_count)

def get_post_historic_comment_data(comments:list, created:str, minutes:str):
    created = format_time(created)
    time_change = timedelta(minutes=minutes)
    max_time = created + time_change

    max_reputation = 0
    top_value_comment = 0
    mean_comment_value = 0
    mean_comment_votes = 0
    total_comments = 0
    for comment in comments:
        time = format_time(comment['created'])
        if time <= max_time:
            comment_minutes = max_time - time
            total_comments += 1
            reputation = get_displayed_reputation(int(comment['author_reputation']))
            if reputation > max_reputation:
                max_reputation = reputation
            value = get_post_historic_value(post=comment, minutes=comment_minutes)
            mean_comment_value += value
            if value > top_value_comment:
                top_value_comment = value
            mean_comment_votes += get_post_historic_vote_count(post=comment, minutes=comment_minutes)
    if total_comments != 0:
        mean_comment_value = mean_comment_value / total_comments
        mean_comment_votes = mean_comment_votes // total_comments
    else:
        mean_comment_value = 0.00
        mean_comment_votes = 0

    return total_comments, float("{:.2f}".format(mean_comment_value)), mean_comment_votes, int(max_reputation), float("{:.2f}".format(top_value_comment))

def get_voter_value(total_value:int, total_rshares:int, voter_rshares:int) -> float:
    if total_rshares != 0:
        voter_value = (voter_rshares/total_rshares) * total_value
    else:
        voter_value = 0
    return float("{:.2f}".format(voter_value))

def get_voter_stats(voter:str, post:dict, total_value:float):
    total_rshares = get_post_historic_rshares(post['created'], 10080, post['active_votes'])
    for vote in post['active_votes']:
        if vote['voter'] == voter:
            value = get_voter_value(total_value=total_value, total_rshares=total_rshares, voter_rshares=int(vote['rshares']))
            rshares = int(vote['rshares'])
            percent = int(vote['percent'])
            return value, rshares, percent

    return 0, 0, 0

def get_post_historic_rshares(post:dict, minutes:int) -> float:
    created = post['created']
    active_votes = post['active_votes']
    historic_rshares = 0
    for vote in active_votes:
        if track_vote_time(vote=vote, minutes=minutes, created=created):
            historic_rshares += int(vote['rshares'])
        else:
            break
    return historic_rshares

def order_voters(voters:list, total_value:float, total_rshares:int, post:dict) -> list:
    values = []
    sorted = []
    for x in range(len(voters)):
        voter = voters[x]
        for vote in post['active_votes']:
            if vote['voter'] == voter:
                voter_rshares = int(vote['rshares'])
        voter_value = get_voter_value(total_value=total_value, total_rshares=total_rshares, voter_rshares=voter_rshares)
        values.append(voter_value)
    for y in range(len(values)):
        greatest = 0
        if len(values) > 1:
            for x in range(len(values)):
                if values[x] > values[greatest]:
                    greatest = x
        if len(values) > 0:
            sorted.append(voters[greatest])
            values.pop(greatest)
            voters.pop(greatest)
        else:
            break
    return sorted

def get_post_historic_top_voter(post, minutes) -> str:
    active_votes = post['active_votes']
    voters = []
    total_value = get_post_historic_value(post=post, minutes=minutes)
    total_rshares = get_post_historic_rshares(post=post, minutes=minutes)
    for vote in active_votes:
        decision = track_vote_time(vote=vote, minutes=minutes, created=post['created'])
        if decision == True:
            voter = vote['voter']
            voters.append(voter)
        else:
            break
    if len(voters) > 0:
        sorted = order_voters(voters=voters, total_value=total_value, total_rshares=total_rshares, post=post)
        top_voter = sorted[0]
        for vote in active_votes:
            if vote['voter'] == top_voter:
                rep = vote['reputation']
                percent = vote['percent']
        return top_voter
    else:
        return None

def get_post_historic_median_voter(post, minutes) -> str:
    active_votes = post['active_votes']
    voters = []
    total_value = get_post_historic_value(post=post, minutes=minutes)
    total_rshares = get_post_historic_rshares(post=post, minutes=minutes)
    for vote in active_votes:
        decision = track_vote_time(vote=vote, minutes=minutes, created=post['created'])
        if decision == True:
            voter = vote['voter']
            voters.append(voter)
        else:
            break
    if len(voters) > 0:
        sorted = order_voters(voters=voters, total_value=total_value, total_rshares=total_rshares, post=post)
        top_voter = sorted[len(sorted)//2]
        for vote in active_votes:
            if vote['voter'] == top_voter:
                rep = vote['reputation']
                percent = vote['percent']
        return top_voter
    else:
        return None

