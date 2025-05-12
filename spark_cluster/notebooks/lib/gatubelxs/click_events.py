import random
import uuid
from datetime import datetime
from .products import sample_product

def generate_click_event(user_id, session_id):
    p = sample_product()
    return {
        "user_id": user_id,
        "session_id": session_id,
        "element_id": random.choice([
            "btn_add_to_cart", "link_product",
            "img_banner", "nav_login", "btn_checkout","btn_buy"
        ]),
        "page_url": f"/product/{p['product_id']}",
        "category": p["category"],
        "price": p["price"],
        "timestamp": datetime.now().isoformat(),
        "x_coord": round(random.uniform(0, 1920), 2),
        "y_coord": round(random.uniform(0, 1080), 2)
    }
