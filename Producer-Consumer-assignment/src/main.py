from typing import List
from .blocking_queue import BlockingQueue
from .producer import Producer
from .consumer import Consumer

def main():
    """ Initialize all objects and start the threads for producer and consumer and join them """
    # Example source and destination containers
    source_data = list(range(1, 11))  # e.g., items 1..10
    destination_data: List[int] = []

    # Shared blocking queue
    bq = BlockingQueue(maxSize=4)

    # Create producer and consumer threads
    producer = Producer(queue=bq, source=source_data, delay=0.0001)
    consumer = Consumer(queue=bq, destination=destination_data, delay=0.0001)

    # Start threads
    producer.start()
    consumer.start()

    # Wait for both to finish
    producer.join()
    consumer.join()

    print("Final destination data:", destination_data)


if __name__ == "__main__":
    main()
