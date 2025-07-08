import itertools
import threading

class ManagerKey:
    def __init__(self, listKey: list):
        if not listKey:
            raise ValueError("listKey must contain at least one key.")
        if not isinstance(listKey, list):
            listKey = [listKey]
        self.listKey = listKey
        self.lock = threading.Lock()
        self.counter = itertools.cycle(range(len(self.listKey)))
    
    def getKey(self):
        with self.lock:
            index = next(self.counter)
            return self.listKey[index]