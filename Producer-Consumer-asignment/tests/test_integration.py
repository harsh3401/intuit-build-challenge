import unittest
from src.producer import Producer
from src.consumer import Consumer
from src.blocking_queue import BlockingQueue

class TestIntegration(unittest.TestCase):
    def test_full_workflow(self):
        """Verify data integrity from source to destination."""
        source = list(range(1, 20)) # 19 items
        destination = []
        bq = BlockingQueue(maxSize=4) # Small queue to force context switching

        p = Producer(bq, source, delay=0.001)
        c = Consumer(bq, destination, delay=0.001)

        p.start()
        c.start()

        p.join(timeout=5.0)
        c.join(timeout=5.0)

        self.assertFalse(p.is_alive(), "Producer stuck")
        self.assertFalse(c.is_alive(), "Consumer stuck")
        
        # Data integrity check
        self.assertEqual(source, destination)

    def test_concurrent_stress(self):
        """Larger dataset to check for race conditions."""
        source = list(range(1000))
        destination = []
        bq = BlockingQueue(maxSize=10)

        p = Producer(bq, source, delay=0.0)
        c = Consumer(bq, destination, delay=0.0)

        p.start()
        c.start()

        p.join(timeout=5.0)
        c.join(timeout=5.0)

        self.assertEqual(len(destination), 1000)
        self.assertEqual(source, destination)