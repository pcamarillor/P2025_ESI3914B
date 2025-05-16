from time import sleep
from datetime import datetime
import random
from faker import Faker

fake = Faker('es_MX')

def generate_random_tweets(n: int, user_id: int):
    tweets = []

    for _ in range(n):
        # Fecha y hora aleatoria entre el 1 de enero de 2024 y el 1 de mayo de 2025 (Para evitar antigüedades de horas, minutos, segundos)
        ts_dt = fake.date_time_between(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2025, 5, 1)
        )

        tweets.append({
            'tweet_id':       fake.uuid4(),
            'user_id':        user_id,
            'timestamp':      ts_dt.isoformat(),
            'text':           fake.sentence(nb_words=random.randint(5, 15)),
            'hashtags':       [fake.word() for _ in range(random.randint(0, 3))],
            'mentions':       [fake.user_name() for _ in range(random.randint(0, 2))],
            'retweet_count':  random.randint(0, 500),
            'favorite_count': random.randint(0, 1000),
            'reply_count':    random.randint(0, 100),
            'quote_count':    random.randint(0, 50),
            'views':          random.randint(100, 20000),
        })

    return tweets