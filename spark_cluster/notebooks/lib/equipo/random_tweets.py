from time import sleep
from datetime import datetime
import random
import os
import json
from faker import Faker

# Configuración del directorio de salida
#source_dir = "/home/jovyan/notebooks/data/tweet_streaming/input/"
source_dir = "./dates"
os.makedirs(source_dir, exist_ok=True)

sentiment_choices = []

# Inicializar Faker para datos en español
default_locale = 'MX'
fake = Faker(default_locale)

while True:
    # Cantidad de tweets a generar en este lote
    n = random.randint(30, 100)
    tweets = []

    for _ in range(n):
        tweets.append({
            'tweet_id':      fake.uuid4(),
            'user_id':       fake.uuid4(),
            'timestamp':     fake.date_time_between_dates(datetime(2024, 1, 1), datetime(2025, 5, 1)).isoformat(),
            'text':          fake.sentence(nb_words=random.randint(5, 15)),
            'hashtags':      [fake.word() for _ in range(random.randint(0, 3))],
            'mentions':      [fake.user_name() for _ in range(random.randint(0, 2))],
            'retweet_count': random.randint(0, 50),
            'favorite_count':random.randint(0, 200),
            'reply_count':   random.randint(0, 20),
            'quote_count':   random.randint(0, 10),
            'is_retweet':    random.choice([True, False]),
            'language':      random.choice(['es', 'en', 'fr']),
            'location':      fake.city(),
            'was_viral':     "Formula magi"
        })

    # Nombre de archivo por lote: tweets-<timestamp>.json
    ts_str = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f"tweets-{ts_str}.json"
    path = os.path.join(source_dir, filename)

    # Guardar cada tweet en una línea JSON
    with open(path, 'w') as f:
        for tweet in tweets:
            f.write(json.dumps(tweet) + "\n")

    print(f"Archivo generado: {path} con {n} tweets simulados")
    # Espera antes del siguiente lote
    sleep(25)