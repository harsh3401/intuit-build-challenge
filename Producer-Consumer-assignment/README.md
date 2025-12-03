# Producer-Consumer Pattern Implementation

1. Create the buffer class and its operations ( Shared queue )
2. Create a producer and consumer class with src and destination containers tied here ( can be abstracted out as well )
3. Create the core logic for the same for handling init data and join everything in the main thread


## Given Instructions 

### Testing Objectives:
    • Thread synchronization
    • Concurrent programming
    • Blocking queues
    • Wait/Notify mechanism

• Write comprehensive unit tests
• Document code with comments

## Run instructions

- ./scripts/setup_env.sh to run the python env setup 
- python3 main.py to run the main program
- `python3 -m unittest discover tests` - All tests 
- `python3 -m unittest tests.test_producer` - Individual tests