from datetime import datetime

def current_timestamp():

    return datetime.now()

def current_date():

    return datetime.now().date()

def execution_seconds(start, end):

    return int(
        (end - start).total_seconds()
    )

def year():

    return datetime.now().strftime("%Y")

def month():

    return datetime.now().strftime("%m")

def day():

    return datetime.now().strftime("%d")