import threading
from steem import Steem
from steembase.exceptions import RPCError
from steem import blockchain
from time import time, sleep
from datetime import datetime, timedelta
from Downloading.SteemSQL import SSQL
from Downloading.SteemPostData import get_post_data
from Downloading.SteemPostData import check_post
from multiprocessing import Pool
import requests.exceptions

s = Steem()
b = blockchain.Blockchain()
db = SSQL()

def downloadPosts(start:int=36000000, end:int=None):
    total_time = 0
    total_download_posts = 0
    try:
        chain = blockchain.Blockchain()
    except Exception as e:
        print(e)
        sleep(5)
    while start > -1:
        try:
            start_time = time()
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
                        except Exception as e:
                            print('inner exception')
                            print(e)
                            sleep(5)
                            print('inner done sleeping')
                    if check_post(post):
                        '''end_time = time()
                        total_time += end_time - start_time
                        total_download_posts += 1
                        print(f'{end_time - start_time}s avg: {5 * (total_time / total_download_posts)}s')'''
                        unprocessed_posts.append((post, block))
                        print(f'{len(unprocessed_posts)} posts downloaded')
                        #start_time = time()
        except Exception as e:
            start += 1
            print('outer exception')
            print(e)
            sleep(5)
            print('outer done sleeping')

def processPosts(event):
    total_processing_time = 0
    total_processed_posts = 0
    total_insertion_time = 0
    total_inserted_posts = 0
    total_insertions = 0
    posts_processed = 0
    processed_posts = []
    total_batch_time = 0
    total_batch_size = 0
    while True:
        batch_start = time()
        event.wait(15)
        posts_to_process = []
        for x in range(len(unprocessed_posts)):
            posts_to_process.append(unprocessed_posts.pop(0))
        start = time()
        try:
            with Pool(5) as p:
                returned_posts = p.map(poolProcessPosts, posts_to_process)
                for post in returned_posts:
                    processed_posts.append(post)
                    print(f"{len(processed_posts)}. Date: {post['created']} | Length {post['body_word_count']} | Total Value: {post['total_value']} | Author Average {post['author_avg_payout_1_week']}")
        except Exception as e:
            print(e)
        end = time()
        posts_processed += len(posts_to_process)
        processed = end-start
        total_processing_time += processed
        total_processed_posts += len(posts_to_process)
        print(f"{len(posts_to_process)} posts processed | Processed in: {processed}s | Average Processing Time: {total_processing_time/total_processed_posts}s")
        batch_end = time()
        total_insertion_time += insertPosts('TrainingData3', processed_posts)
        total_inserted_posts += len(processed_posts)
        total_insertions += 1
        total_batch_time += batch_end - batch_start
        total_batch_size += 1
        print(f'Average Insertion time {total_insertion_time/total_insertions} | Batch processed in {batch_end - batch_start}s | Batch Avg: {total_batch_time/total_batch_size}s | Avg Batch Size: {total_inserted_posts // total_batch_size}')
        batch_start = time()
        processed_posts = []
        posts_processed = 0


def poolProcessPosts(item):
    post, block = item
    return get_post_data(post, block['block_num'], df, processed_posts)

def insertPosts(table, posts):
    total_inserting_time = 0
    total_inserted_posts = 0
    print('insert')
    try:
        start = time()
        df = db.insert_training_articles(table, posts)
        end = time()
        inserted = end-start
        total_inserting_time += inserted
        print(f"Length of DF: {len(df)} | {len(posts)} posts inserted in: {inserted}s")
        return inserted
    except Exception as e:
        print(e)

class downloadThread(threading.Thread):
    def __init__(self, threadId, name):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.name = name

    def run(self):
        print(f'starting {self.name}')
        downloadPosts(df.iloc[1]['block'])


class processThread(threading.Thread):
    def __init__(self, threadId, name):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.name = name

    def run(self):
        event = threading.Event()
        print(f'starting {self.name}')
        processPosts(event)

if __name__ == '__main__':
    unprocessed_posts = []
    processed_posts = []
    posts_processed = 0
    df = db.get_data('TrainingData3')
    print(df.head())
    print(len(df))
    download = downloadThread(1, 'Download')
    process = processThread(2, 'Process')

    #63133677

    download.start()
    process.start()
    download.join()
    process.join()

#98716 8:16am


    