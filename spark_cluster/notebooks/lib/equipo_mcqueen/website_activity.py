from faker import Faker
import random
from datetime import datetime

fake = Faker()

def generate_website_event():
    return {
        "event_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "page_url": fake.uri(),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "action_type": random.choice(['view', 'click', 'scroll']),
        "browser": fake.user_agent(),
        "timezone": fake.timezone(),
        "device_type": random.choice(['mobile', 'desktop', 'tablet']),
        "click_coordinates_x": random.randint(0, 1920),
        "click_coordinates_y": random.randint(0, 1080),
        "session_id": fake.uuid4(),
        "ip_address": fake.ipv4()
    }