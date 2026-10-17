# Concurrent Message Queue

A thread-safe message queue implemented in C++17 as part of an Operating Systems course project, designed to handle concurrent message production and consumption using synchronization mechanisms.

The implementation follows the **Producer–Consumer model**, ensuring FIFO message processing, safe access to shared resources, and efficient coordination between multiple threads.

## Features

- **FIFO Processing:** Messages are retrieved in the order they are inserted.
- **Concurrent Producers and Consumers:** Supports multiple threads sending and receiving messages simultaneously.
- **Blocking and Non-blocking Retrieval:** Provides both waiting and immediate-return message retrieval operations.
- **Bounded and Unbounded Queues:** Supports configurable queue capacity, rejecting new messages when a bounded queue is full.
- **Queue Statistics:** Tracks message count, total message size, and process memory usage.

## Implementation

The queue uses a **two-lock design**, with separate mutexes for insertion and removal to reduce contention between producers and consumers.

A semaphore manages message availability, while an additional mutex protects queue statistics and capacity constraints. Atomic operations ensure safe publication of newly inserted nodes.

## Testing

The implementation was tested for:

- Concurrent processing of 10,000 messages across 20 threads.
- Queue capacity enforcement.
- Blocking and non-blocking retrieval.
- FIFO ordering and message integrity.


