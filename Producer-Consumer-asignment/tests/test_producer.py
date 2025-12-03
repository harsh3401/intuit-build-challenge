import unittest
import threading
from src.producer import Producer
from src.blocking_queue import BlockingQueue

class TestProducer(unittest.TestCase):
    def setUp(self):
        self.bq = BlockingQueue(maxSize=5)
        self.source = [1, 2, 3]

    def test_producer_lifecycle(self):
        """Test that producer moves all items + sentinel to queue."""
        # Setting delay to 0 for fast testing
        producer = Producer(self.bq, self.source, delay=0.0)
        producer.start()
        producer.join(timeout=2.0)

        self.assertFalse(producer.is_alive())

        # Verify content of queue
        items_in_queue = []
        # We expect 3 items + 1 sentinel = 4 items
        for _ in range(4):
            items_in_queue.append(self.bq.get())

        self.assertEqual(items_in_queue, [1, 2, 3, None])

    def test_empty_source(self):
        """Test producer behavior with empty source."""
        producer = Producer(self.bq, [], delay=0.0)
        producer.start()
        producer.join()
        
        # Should only contain sentinel
        self.assertIsNone(self.bq.get())