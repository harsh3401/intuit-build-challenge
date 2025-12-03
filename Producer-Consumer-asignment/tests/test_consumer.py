import unittest
from src.consumer import Consumer
from src.blocking_queue import BlockingQueue

class TestConsumer(unittest.TestCase):
    def setUp(self):
        self.bq = BlockingQueue(maxSize=5)
        self.destination = []
        self.consumer = Consumer(self.bq, self.destination, delay=0.0)

    def test_consumer_consumption(self):
        """Test that consumer drains queue until sentinel."""
        # Pre-fill queue
        self.bq.put(10)
        self.bq.put(20)
        self.bq.put(None) # Sentinel

        self.consumer.start()
        self.consumer.join(timeout=2.0)

        self.assertFalse(self.consumer.is_alive())
        self.assertEqual(self.destination, [10, 20])

    def test_immediate_sentinel(self):
        """Test consumer receiving immediate stop signal."""
        self.bq.put(None)
        self.consumer.start()
        self.consumer.join(timeout=1.0)
        self.assertEqual(self.destination, [])