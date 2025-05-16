import random
import uuid
from datetime import datetime
from .products import sample_product

def generate_click_event(user_id, session_id, user_stage = "browse"):
    p = sample_product(user_id)

    #Simulate a clickc event based on a "funnel" stage

    if user_stage == "browse":
        element_id = random.choices(["link_product", "img_banner", "btn_add_to_cart"], weights=[0.5, 0.3, 0.2])[0]
    elif user_stage == "cart":  
        element_id = random.choices(["btn_checkout", "btn_add_to_cart"], weights=[0.6, 0.4])[0]
    elif user_stage == "checkout":
        element_id = random.choices(["btn_buy", "btn_checkout"], weights=[0.7,0.3])[0]
    else:
        element_id = random.choices([
            "btn_add_to_cart", "link_product",
            "img_banner", "nav_login", "btn_checkout","btn_buy"
        ])[0]

    return {
        "user_id": user_id,
        "session_id": session_id,
        "element_id": element_id,
        "page_url": f"/product/{p['product_id']}",
        "category": p["category"],
        "price": p["price"],
        "timestamp": datetime.now().isoformat(),
        "x_coord": round(random.uniform(0, 1920), 2),
        "y_coord": round(random.uniform(0, 1080), 2)
    }
