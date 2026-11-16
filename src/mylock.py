from src.readerwriterlock import rwlock

class Lock:
    def __init__(self):
        self.lock = rwlock.RWLockWrite()
        self.read = self.lock.gen_rlock()
        self.write = self.lock.gen_wlock()