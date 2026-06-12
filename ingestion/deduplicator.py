# ingestion/deduplicator.py
import hashlib

class Deduplicator:
    """Prevent duplicate processing when multiple sources detect the same filing."""
    
    def __init__(self):
        self.seen: set[str] = set()
    
    def is_new(self, content: str) -> bool:
        h = hashlib.sha256(content[:2000].encode()).hexdigest()
        if h in self.seen:
            return False
        self.seen.add(h)
        return True