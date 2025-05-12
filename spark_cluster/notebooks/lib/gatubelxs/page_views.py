from faker import Faker
import uuid
import random
from datetime import datetime
from .products import sample_product


fake = Faker()

def generate_page_view(user_id, session_id):
    '''
    Method to generate the tracking data of a page view
    '''
    p = sample_product(user_id)
    return {
        "user_id": user_id,
        "session_id": session_id,
        "page_url": f"/product/{p['product_id']}",
        "referrer_url": fake.uri_path(),
        "category": p["category"],
        "price": p["price"],
        "timestamp": datetime.now().isoformat()
    }
