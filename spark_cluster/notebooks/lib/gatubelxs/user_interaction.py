from random import randint, choice
from datetime import datetime
from .products import sample_product

def generate_user_interaction(user_id, session_id):
    p = sample_product(user_id)
    interaction_type = choice(["scroll",
                                      "hover",
                                      "form_submit",
                                      "zoom",
                                      "copy_text"])
    details = {}
    if interaction_type == "scroll":
        details["scroll_depth"] = randint(10,100)
    elif interaction_type == "hover":
        details["element"] = choice(
            ["img_banner", "product_card", "nav_link"]
        )
    elif interaction_type == "form_submit":
        details["form_id"] = "signup_for"
    elif interaction_type == "zoom":
        details["zoom_level"] = choice([100,
                                        125, 150])
    elif interaction_type == "copy_text":
        details["length"] = randint(10,100)

    return {
        "user_id": user_id,
        "session_id": session_id,
        "interaction_type": interaction_type,
        "page_url": f"/product/{p['product_id']}",
        "category": p["category"],
        "price": p["price"],
        "details": details,
        "timestamp": datetime.now().isoformat()
    }
