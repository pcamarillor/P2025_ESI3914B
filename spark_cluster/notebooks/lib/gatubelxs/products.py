import random

# Fixed product pool
PRODUCT_POOL = {
    "electronics": [f"elect_{i}" for i in range(1000, 1010)],
    "clothing":    [f"cloth_{i}" for i in range(2000, 2010)],
    "home":        [f"home_{i}" for i in range(3000, 3010)],
    "books":       [f"book_{i}" for i in range(4000, 4010)],
    "toys":        [f"toy_{i}" for i in range(5000, 5010)],
}

def get_user_profile(user_id):
    # Consistent profile per user
    random.seed(user_id)
    categories = list(PRODUCT_POOL.keys())
    preferred_category = random.choice(categories)
    min_price = random.choice([10, 20, 50, 100])
    max_price = min_price + random.choice([100, 200, 400])
    return {"preferred_category": preferred_category, "price_range": (min_price, max_price)}

def sample_product(user_id=None):
    if user_id:
        prefs = get_user_profile(user_id)
        category = prefs["preferred_category"]
        min_price, max_price = prefs["price_range"]
    else:
        category = random.choice(list(PRODUCT_POOL.keys()))
        min_price, max_price = 10, 1000

    product_id = random.choice(PRODUCT_POOL[category])
    price = round(random.uniform(min_price, max_price), 2)
    
    return {
        "product_id": product_id,
        "category": category,
        "price": price
    }
