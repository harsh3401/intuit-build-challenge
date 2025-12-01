import threading
import time
from typing import List, Any
from blocking_queue import BlockingQueue

class Consumer(threading.Thread):
    """Consumer thread: dequeues items and writes them to a destination container."""

    def __init__(self, queue: BlockingQueue, destination: List[Any], delay: float = 0.15):
        super().__init__()
        self.queue = queue
        self.destination = destination
        self.delay = delay

    def run(self) -> None:
        while True:
            item = self.queue.get()
            if item is None:
                # TODO: Put sentinel back if we  have multiple consumers
                # self.queue.put(None)
                print("(Consumer) Received sentinel, stopping.")
                break
            # Simulatuing processing work
            print(f"(Consumer) Consuming: {item}")
            time.sleep(self.delay)
            self.destination.append(item)

