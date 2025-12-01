import threading
import time
from typing import List, Any
from blocking_queue import BlockingQueue

class Producer(threading.Thread):
    """Producer thread: reads from a source container and enqueues items."""

    def __init__(self, queue: BlockingQueue, source: List[Any], delay: float = 0.1):
        super().__init__()
        self.queue = queue
        self.source = source
        self.delay = delay

    def run(self) -> None:
        for item in self.source:
            # Simulating item production
            time.sleep(self.delay)
            print(f"(Producer) Producing: {item}")
            self.queue.put(item)
        # Send sentinel to signal consumer to stop
        print("(Producer) Sending sentinel, done.")
        self.queue.put(None)  # sentinel value to signal consumer to stop
