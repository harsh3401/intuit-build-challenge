import threading
import time
from typing import Any, List


class BlockingQueue:
    """
    Bounded blocking queue.
    Lock + Condition + wait()/notify() for synchronization.
    We keep two conditions so that we can have two waiting queues for producer and consumer each.
    """

    def __init__(self, maxSize: int):
        self.maxSize = maxSize
        self.queue: List[Any] = []
        self.lock = threading.Lock()
        self.notEmpty = threading.Condition(self.lock)
        self.notFull = threading.Condition(self.lock)

    def put(self, item: Any) -> None:
        """Put an item into the queue, blocking if the queue is full."""
        with self.notFull:
            while len(self.queue) >= self.maxSize:
                # Wait until a consumer removes an item
                self.notFull.wait()
            self.queue.append(item)
            # Signal that the queue is no longer empty
            self.notEmpty.notify()

    def get(self) -> Any:
        """Remove and return an item from the queue, blocking if empty."""
        with self.notEmpty:
            while len(self.queue) == 0:
                # Wait until a producer adds an item
                self.notEmpty.wait()
            item = self.queue.pop(0)
            # Signal that there is now space for producers
            self.notFull.notify()
            return item

