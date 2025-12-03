import unittest
import threading
import time
from src.blocking_queue import BlockingQueue

class TestBlockingQueue(unittest.TestCase):
    def setUp(self):
        # Create a fresh queue with size 3 for every test
        self.bq = BlockingQueue(maxSize=3)

    def test_basic_put_get(self):
        """Test simple FIFO order without blocking."""
        self.bq.put(1)
        self.bq.put(2)
        self.assertEqual(self.bq.get(), 1)
        self.assertEqual(self.bq.get(), 2)

    def test_blocking_when_empty(self):
        """Ensure get() blocks when queue is empty."""
        def consumer_thread():
            self.bq.get()

        t = threading.Thread(target=consumer_thread)
        t.start()
        
        # Give the thread a moment to hit the wait() state
        time.sleep(0.05)
        
        # Verify thread is still alive (it should be blocked waiting)
        self.assertTrue(t.is_alive(), "Thread should be blocked on empty queue")
        
        # Unblock it
        self.bq.put("Item")
        t.join(timeout=1.0)
        self.assertFalse(t.is_alive(), "Thread should finish after item is added")

    def test_blocking_when_full(self):
        """Ensure put() blocks when queue is full."""
        # Fill the queue
        for i in range(3):
            self.bq.put(i)

        def producer_thread():
            self.bq.put(4)

        t = threading.Thread(target=producer_thread)
        t.start()

        time.sleep(0.05)
        self.assertTrue(t.is_alive(), "Thread should be blocked on full queue")

        # Unblock by removing an item
        self.bq.get()
        t.join(timeout=1.0)
        self.assertFalse(t.is_alive(), "Thread should finish after space is made")