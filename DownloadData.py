from steem import Steem
from steembase.exceptions import RPCError
from steem import blockchain
from time import time, sleep
from datetime import datetime, timedelta
from SteemSQL import SSQL
from SteemPostData import get_post_data
from Downloading.SteemPostData import check_post
import requests.exceptions

'''
api.steemit.com
api.steemitdev.com

cd /etc
sudo vi resolv.conf

'''
s = Steem()
b = blockchain.Blockchain()
FILE = "../Data/Training1.txt"
DB = SSQL()

#mysql -h localhost -P 3306 -u cmp2020 SteemSQL -p Password: Schubert1-31

def store_articles_stream(start:int=36000000, end:int=None, max:int=100000000, df=None, insert:int=30):
    n = 1
    chain = blockchain.Blockchain()
    total_processing = 0
    total_inserting = 0
    count = 0
    articles = []
    while start > -1:
        for block in chain.history(filter_by="comment", start_block=start, end_block=end):
            start += 1
            if block["parent_author"] == "":
                start = block['block_num']
                author = block["author"]
                permlink = block["permlink"]
                while True:
                    try:
                        post = s.get_content(author, permlink)
                        break
                    except RPCError as e:
                        print(e)
                        sleep(1)
                format_string = '%Y-%m-%dT%H:%M:%S'
                original_created = post['created']
                created = datetime.strptime(original_created, format_string)
                most = datetime.now() - timedelta(days=7)
                base = datetime.strptime("2019-08-27T23:59:59", format_string)
                if created >= base and created <= most:
                    if n >= 0:
                        if check_post(post):
                            first = time()
                            try:
                                post = get_post_data(post, block['block_num'], df, articles)
                            except requests.exceptions.ConnectionError as e:
                                print(e)
                            last = time()
                            processed = last - first
                            total_processing += processed
                            articles.append(post)
                            print(len(articles), f"Date: {post['created']} Processed in {processed}s, Total Value: {post['total_value']} Author_avg: {post['author_avg_payout_1_week']}")
                            count += 1
                            if len(articles) >= insert:
                                try:
                                    first = time()
                                    df = DB.insert_training_articles("TrainingData2", articles)
                                    last = time()
                                    inserted = last - first
                                    total_inserting += inserted
                                    print(len(df), f" Inserted in {inserted}s")
                                    print(f"Average processing time: {total_processing / count}")
                                    print(f"Average inserting time: {total_inserting / (count / insert)}")
                                except Exception as e:
                                    print(e)
                                articles = []
                            '''try:
                                first = time()
                                df = DB.insert_training_article(table="TrainingData2", article=post)
                                last = time()
                                inserted = last - first
                                total_inserting += inserted
                                n = len(df)
                                total_value = df['total_value'].sum()
                                print(f"{n} Date: {post['created']} Processed in {processed}s, Inserted in {inserted}s, Total Value: {post['total_value']} Author_avg: {post['author_avg_payout_1_week']}")
                                print(f"Average value: {total_value/n}")
                                print(f"Average processing time: {total_processing/count}")
                                print(f"Average insertion time: {total_inserting / count}")
                            except Exception as e:
                                print(e)'''

                if n == max:
                    print('max reached')
                    exit()

post = s.get_content('cmp2020', 'building-mod-bot-an-experimental-community-moderation-tool')
df = DB.get_data('TrainingData2')
print(df.head())
print(len(df))
block = df['block'].iloc[0]


#block = 62144136
store_articles_stream(start=block, df=df, insert=100)


#1655. Date: 2022-05-30T12:04:57 Processed in 5.924251079559326s, Total Value: 4.95 Author_avg: 15.181
