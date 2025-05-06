from time import sleep
from datetime import datetime
import random
import os
import json
import math
from faker import Faker

source_dir = "./dates"
os.makedirs(source_dir, exist_ok=True)

fake = Faker('es_MX')

# Pesos para la fórmula
w1, w2, w3 = 2.0, 1.5, 1.0

while True:
    n = random.randint(30, 100)
    tweets = []

    for _ in range(n):
        
        ts_dt = fake.date_time_between_dates(
            datetime_start=datetime(2024, 1, 1),
            datetime_end=datetime(2025, 5, 1)
        )
        retweets  = random.randint(0, 500)
        favorites = random.randint(0, 1000)
        replies   = random.randint(0, 100)
        quotes    = random.randint(0, 50)
        views     = random.randint(100, 20000)

        tweets.append({
            'tweet_id':       fake.uuid4(),
            'user_id':        fake.uuid4(),
            'timestamp':      ts_dt.isoformat(),
            'text':           fake.sentence(nb_words=random.randint(5, 15)),
            'hashtags':       [fake.word() for _ in range(random.randint(0, 3))],
            'mentions':       [fake.user_name() for _ in range(random.randint(0, 2))],
            'retweet_count':  retweets,
            'favorite_count': favorites,
            'reply_count':    replies,
            'quote_count':    quotes,
            'views':          views,
        })

    # 2) Indice de viralidad
    now = datetime.now()
    raw_scores = []
    for t in tweets:
        posted = datetime.fromisoformat(t['timestamp'])
        hours_passed = (now - posted).total_seconds() / 3600.0
        reactions = t['favorite_count'] + t['quote_count']

        raw = (
            w1 * t['retweet_count']
          + w2 * t['reply_count']
          + w3 * reactions
        ) / (hours_passed + 1) * math.log(t['views'] + 1)

        raw_scores.append(raw)

    # 3) Normaliza al rango 1–10
    min_raw = min(raw_scores)
    max_raw = max(raw_scores)
    for t, raw in zip(tweets, raw_scores):
        if max_raw > min_raw:
            score_1_10 = int(1 + (raw - min_raw) / (max_raw - min_raw) * 9)
        else:
            score_1_10 = 1
        t['viral'] = score_1_10

    # 4) Guardar
    ts_str  = now.strftime('%Y%m%d%H%M%S')
    fname   = f"tweets-{ts_str}.json"
    outpath = os.path.join(source_dir, fname)
    with open(outpath, 'w') as f:
        for tweet in tweets:
            f.write(json.dumps(tweet) + "\n")

    print(f"Archivo generado: {outpath} con {n} tweets simulados")
    sleep(25)
