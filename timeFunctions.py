from datetime import datetime, timedelta

def convertTime(date):
    return datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')

def convertTimeStamp(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%d-%m-%Y')


def getTime(date):
    return int(round(date.timestamp()))