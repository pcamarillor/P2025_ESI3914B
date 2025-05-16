from datetime import datetime
import random

def generate_user_id(platform):
    prefixes = {
        'Instagram': 'ig',
        'Twitter': 'tw',
        'Facebook': 'fb',
        'TikTok': 'tt',
    }
    return f"user_{prefixes[platform]}_{random.randint(1, 500)}"

def generate_instagram_data():
    return {
        'platform': 'Instagram',
        'user_id': generate_user_id('Instagram'),
        'post_id': f'post_{random.randint(1000, 9999)}',
        'event_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'likes': random.randint(0, 5000),
        'comments': random.randint(0, 500),
        'shares': random.randint(0, 100)
    }

def generate_twitter_data():
    return {
        'platform': 'Twitter',
        'user_id': generate_user_id('Twitter'),
        'tweet_id': f'tweet_{random.randint(10000, 99999)}',
        'event_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'likes': random.randint(0, 2000),
        'retweets': random.randint(0, 500),
        'replies': random.randint(0, 300)
    }

def generate_facebook_data():
    return {
        'platform': 'Facebook',
        'user_id': generate_user_id('Facebook'),
        'post_id': f'post_{random.randint(2000, 9999)}',
        'event_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'likes': random.randint(0, 4000),
        'comments': random.randint(0, 700),
        'shares': random.randint(0, 200)
    }

def generate_tiktok_data():
    return {
        'platform': 'TikTok',
        'user_id': generate_user_id('TikTok'),
        'video_id': f'vid_{random.randint(3000, 99999)}',
        'event_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'likes': random.randint(0, 8000),
        'shares': random.randint(0, 300),
        'comments': random.randint(0, 600)
    }


def generate_social_data():
    generators = [
        generate_instagram_data,
        generate_twitter_data,
        generate_facebook_data,
        generate_tiktok_data,
    ]
    return random.choice(generators)()
