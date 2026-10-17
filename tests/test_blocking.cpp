#include "../src/message_queue.h"

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

int main() {
    using Clock = std::chrono::steady_clock;
    using namespace std::chrono;

    MessageQueue queue;
    std::string message;

    // Non-blocking get must return immediately when the queue is empty.
    auto start = Clock::now();
    bool nonblocking_received = queue.nb_msg_get(message);
    auto nonblocking_ms = duration_cast<milliseconds>(Clock::now() - start).count();
    bool nonblocking_pass = !nonblocking_received && nonblocking_ms < 200;

    // Start with an empty queue. The consumer must wait for a producer.
    const std::string expected = "Hello from producer";
    std::string received;
    std::thread producer([&queue, &expected]() {
        std::this_thread::sleep_for(milliseconds(500));
        queue.msg_send(expected);
    });

    start = Clock::now();
    received = queue.msg_get();
    auto blocking_ms = duration_cast<milliseconds>(Clock::now() - start).count();
    producer.join();

    bool blocking_pass = received == expected && blocking_ms >= 350;

    QueueStats info = queue.stats();
    bool empty_pass = info.message_count == 0 && info.total_message_size == 0;
    bool passed = nonblocking_pass && blocking_pass && empty_pass;

    std::cout << "Blocking and non-blocking queue test\n\n";
    std::cout << "Non-blocking get on empty queue: "
              << (nonblocking_received ? "Received unexpectedly" : "No message") << '\n';
    std::cout << "Non-blocking wait time: " << nonblocking_ms << " ms\n";
    std::cout << "Non-blocking test: " << (nonblocking_pass ? "PASSED" : "FAILED") << "\n\n";

    std::cout << "Blocking get received: " << received << '\n';
    std::cout << "Blocking wait time: " << blocking_ms << " ms\n";
    std::cout << "Blocking test: " << (blocking_pass ? "PASSED" : "FAILED") << "\n\n";

    std::cout << "Messages left in queue: " << info.message_count << '\n';
    std::cout << "Total message size: " << info.total_message_size << " bytes\n";
    std::cout << "\nTest result: " << (passed ? "PASSED" : "FAILED") << '\n';

    return passed ? 0 : 1;
}
