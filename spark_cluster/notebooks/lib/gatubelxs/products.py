import random 

NUM_PRODUCTS = 200

PRODUCT_CATALOG = [
    {
        "product_id": pid,
        "category": random.choice(["electronics",
                                   "apparel",
                                   "books",
                                   "home",
                                   "toys"]),

        "price": round(random.uniform(5,500),2)
    }
    for pid in range(1000, 1000 + NUM_PRODUCTS)
]

def sample_product():
    '''
    Pick one procut record at random
    '''
    return random.choice(PRODUCT_CATALOG)
