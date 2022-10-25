from SteemSQL import SSQL
from SteemPostData import get_post_historic_resteems
from AccountInfo import get_author_followers

DB = SSQL()
df = DB.get_data('TrainingData2')
df['permlink'] = df['link']

articles = []
for x in range(len(df)):
    article = df.iloc[x]
    article['resteems'] = get_post_historic_resteems(article, 60)
    article['followers']= get_author_followers(article)
    articles.append(article)
    DB.insert_training_articles('TrainingData2', articles)
