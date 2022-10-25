import pandas as pd
import pycksum
from Downloading.SteemSQL import SSQL
from sklearn.preprocessing import MinMaxScaler
import numpy as np
from random import randint

DB = SSQL()

def create_boolean_total_value(data, point):
    data['total_value_bool'] = data['total_value'] >= point
    data['total_value_bool'] = data['total_value_bool'].astype(int)
    return data

def get_tag_data(df):
    for x in range(1,6):
        temp = df[df[f'tag{x}']!='None']
        df[f'tag{x}_mean_total_value'] = temp.groupby(f'tag{x}')['total_value'].transform('mean')
        df[f'tag{x}_total_posts'] = temp.groupby(f'tag{x}')[f'tag{x}'].transform('count')
        df[f'tag{x}_mean_total_value'] = df[f'tag{x}_mean_total_value'].fillna(0)
        df[f'tag{x}_total_posts'] = df[f'tag{x}_total_posts'].fillna(0)
    return df

def get_rep_data(df):
    df['rep_int'] = df['author_reputation'].astype(int)
    df['author_rep_mean_total_value'] = df.groupby(['rep_int'])['total_value'].transform('mean')
    df['author_rep_total_posts'] = df.groupby(['rep_int'])['rep_int'].transform('count')
    return df

def initial_preprocess_reg(data):
    data = get_tag_data(data)
    data = get_rep_data(data)
    data = data[data['1_hour_value'] <= 1]
    data = data[data['1_hour_value'] > 0]
    drops = ['title', 'link', 'spelling_errors', 'created', 'total_votes', 'total_comments',
             'author_value', 'curation_value', 'beneficiary_value', 'block', 'tag1', 'tag2', 'tag3', 'tag4', 'tag5', 'author']
    for x in drops:
        data = data.drop(x, axis=1)
    dummy1 = pd.get_dummies(data['primary_language'], drop_first=True)
    data = pd.concat([data, dummy1], axis=1).drop('primary_language', axis=1)
    data = data.dropna()
    return data

def initial_preprocess_class(data, point):
    data = data[data['1_hour_value'] <= 1]
    data = data[data['1_hour_value'] > 0]
    print('hillo')
    print(len(data))
    data = create_boolean_total_value(data, point)
    #data = data_augment(data)
    data = get_tag_data(data)
    data = get_rep_data(data)
    data['edited_bool'] = data['edited'].astype(int)
    drops = ['title', 'link', 'spelling_errors', 'created', 'total_votes', 'total_comments',
             'author_value', 'curation_value', 'beneficiary_value', 'block', 'tag1', 'tag2', 'tag3', 'tag4', 'tag5',
             'author']
    for x in drops:
        data = data.drop(x, axis=1)
    dummy1 = pd.get_dummies(data['primary_language'], drop_first=True)
    data = pd.concat([data, dummy1], axis=1).drop('primary_language', axis=1)
    data = data.dropna()
    return data




def preprocess_lstm(train_lstm, test_lstm, lstm_shape):
    columns = ['median_voter', 'top_voter']
    for column in columns:
        train_lstm[column] = train_lstm[column].apply(lambda x: pycksum.cksum(bytes(x, 'utf-8')))
        test_lstm[column] = test_lstm[column].apply(lambda x: pycksum.cksum(bytes(x, 'utf-8')))
    train_lstm = train_lstm.values.astype(float)
    test_lstm = test_lstm.values.astype(float)
    scaler = MinMaxScaler()
    train_lstm = scaler.fit_transform(train_lstm)
    test_lstm = scaler.transform(test_lstm)
    train_lstm = train_lstm.reshape(lstm_shape)
    test_lstm = test_lstm.reshape(lstm_shape)
    return train_lstm, test_lstm

def preprocess_mlp(train_X_mlp, test_X_mlp):
    columns = ['author', 'tag1', 'tag2', 'tag3', 'tag4', 'tag5']
    for column in columns:
        train_X_mlp[column] = train_X_mlp[column].apply(lambda x: pycksum.cksum(bytes(x, 'utf-8')))
        test_X_mlp[column] = test_X_mlp[column].apply(lambda x: pycksum.cksum(bytes(x, 'utf-8')))
    train_X_mlp = train_X_mlp.values.astype(float)
    test_X_mlp = test_X_mlp.values.astype(float)
    scaler = MinMaxScaler()
    train_X_mlp = scaler.fit_transform(train_X_mlp)
    test_X_mlp = scaler.transform(test_X_mlp)
    return train_X_mlp, test_X_mlp

def drop_null_articles(mlp, lstm):
    mlp_ids = mlp[mlp.isnull().any(axis=1)]['id']
    lstm_ids = lstm[lstm.isnull().any(axis=1)]['id']
    mlp = mlp[~mlp['id'].isin(mlp_ids)]
    mlp = mlp[~mlp['id'].isin(lstm_ids)]
    lstm = lstm[~lstm['id'].isin(mlp_ids)]
    lstm = lstm[~lstm['id'].isin(lstm_ids)]
    return mlp, lstm

def get_train_test_mlp_lstm(mlp:pd.DataFrame, lstm:pd.DataFrame, lstm_shape:tuple, test_size:float):
    mlp, lstm = drop_null_articles(mlp, lstm)
    test_mlp = mlp.sample(frac=test_size).sort_values(by=['id'], ascending=True)
    test_X_lstm = lstm[lstm['id'].isin(test_mlp['id'])]
    train_mlp = mlp[~mlp['id'].isin(test_mlp['id'])]
    train_X_lstm = lstm[lstm['id'].isin(train_mlp['id'])]
    train_X_mlp = train_mlp.drop(['total_value', 'id'], axis=1)
    test_X_mlp = test_mlp.drop(['total_value', 'id'], axis=1)
    train_X_lstm = train_X_lstm.drop(['id', 'time'], axis=1)
    test_X_lstm = test_X_lstm.drop(['id', 'time'], axis=1)
    y_train = train_mlp['total_value'].values
    y_test = test_mlp['total_value'].values

    train_X_mlp, test_X_mlp = preprocess_mlp(train_X_mlp, test_X_mlp)
    train_X_lstm, test_X_lstm = preprocess_lstm(train_X_lstm, test_X_lstm, lstm_shape=lstm_shape)
    return train_X_mlp, train_X_lstm, y_train, test_X_mlp, test_X_lstm, y_test

def get_stds(data):
    stds = {}
    bad_columns = ['author', 'tag1', 'tag2', 'tag3', 'tag4', 'tag5', '1_hour_top_voter', '1_hour_median_voter',
                   '5_min_top_voter', '5_min_median_voter', 'primary_language', 'total_value_bool']
    for column in data.columns:
        if column not in bad_columns:
            stds[f"{column}"] = np.std(data[column])

    return stds


def data_augment(data):
    true_values = data[data['total_value_bool'] == 1].reset_index()
    false_values = data[data['total_value_bool'] == 0]
    n = len(false_values) - len(true_values)
    stds = get_stds(data)
    bad_columns = ['tag1', 'tag2', 'tag3', 'tag4', 'tag5', 'primary_language', 'total_value_bool', 'total_value']
    dataset = []

    for _ in range(n):
        print(_)
        index = randint(0, len(true_values) - 1)
        article = true_values.iloc[index]
        temp = {}
        for column in data.columns:
            if column not in bad_columns:
                if randint(0, 1) == 0:
                    temp[f"{column}"] = article[f"{column}"] + np.random.uniform(stds[f"{column}"])
                else:
                    temp[f"{column}"] = article[f"{column}"] - np.random.uniform(stds[f"{column}"])
            else:
                temp[f"{column}"] = article[f"{column}"]
        dataset.append(temp)

    new_data = pd.concat([data, pd.DataFrame(dataset)])
    return new_data

def create_new_data(data, n):
    bad_columns = ['author', 'tag1', 'tag2', 'tag3', 'tag4', 'tag5', '1_hour_top_voter', '1_hour_median_voter',
                   '5_min_top_voter', '5_min_median_voter',
                   'primary_language', 'total_value_bool']
    stds = get_stds(data)

    dataset = []
    for x in range(n):
        print(x)
        index = randint(0, len(data) - 1)
        article = data.iloc[index]
        temp = {}
        for column in data.columns:
            if column not in bad_columns:
                if randint(0, 1) == 0:
                    temp[f"{column}"] = article[f"{column}"] + np.random.uniform(stds[f"{column}"])
                else:
                    temp[f"{column}"] = article[f"{column}"] - np.random.uniform(stds[f"{column}"])
            else:
                temp[f"{column}"] = article[f"{column}"]
        dataset.append(temp)

    new_data = pd.concat([data, pd.DataFrame(dataset)])
    print(new_data)
    return new_data

def get_dataset_partitions_pd(df):
    # Specify seed to always have the same split distribution between runs
    df_sample = df.sample(frac=1)

    train, validation, test = np.split(df_sample, [int(0.6 * len(df)), int(0.8 * len(df))])

    return train, validation, test

