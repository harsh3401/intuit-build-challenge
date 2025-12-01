
class Producer(threading.Thread):
    """Producer: Reads from a source container and enqueues it to the queue."""

    def __init__(self, queue: BlockingQueue, source: List[Any], delay: float = 0.1):

    def run(self) -> None:

