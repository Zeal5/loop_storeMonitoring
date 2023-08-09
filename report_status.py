from cachetools import TTLCache



cache = TTLCache(maxsize=1, ttl= 60 * 60)
