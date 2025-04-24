import os
import pickle
import time

class Cache:
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

    def get(self, key, default=None):
        path = os.path.join(self.cache_dir, key)
        if os.path.exists(path):
            with open(path, 'rb') as f:
                data, timestamp = pickle.load(f)
                return data
        return default

    def set(self, key, data):
        path = os.path.join(self.cache_dir, key)
        with open(path, 'wb') as f:
            pickle.dump((data, time.time()), f)
    
    def exists(self, key):
        path = os.path.join(self.cache_dir, key)
        return os.path.exists(path)
    
    def delete(self, key):
        path = os.path.join(self.cache_dir, key)
        if os.path.exists(path):
            os.remove(path)

    def clear(self):
        for filename in os.listdir(self.cache_dir):
            os.remove(os.path.join(self.cache_dir, filename))

# Usage
# Commented out default cache instance to allow per-user cache instantiation
hcache = Cache('.cache')