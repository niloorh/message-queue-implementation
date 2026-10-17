#include "src/message_queue.h"

#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

std::mutex print_mutex;

void producer(MessageQueue& queue, int id, int count) {
    for (int i = 1; i <= count; ++i) {
        std::string message = "Producer " + std::to_string(id) +
                              " - message " + std::to_string(i);

        if (queue.msg_send(message)) {
            std::lock_guard<std::mutex> lock(print_mutex);
            std::cout << "[Producer " << id << "] sent: " << message << '\n';
        } else {
            std::lock_guard<std::mutex> lock(print_mutex);
            std::cout << "[Producer " << id << "] queue is full\n";
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

void consumer(MessageQueue& queue, int id, int count) {
    for (int i = 0; i < count; ++i) {
        std::string message = queue.msg_get();
        std::lock_guard<std::mutex> lock(print_mutex);
        std::cout << "[Consumer " << id << "] received: " << message << '\n';
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
    }
}

int main() {
    // 0 means unlimited queue size.
    MessageQueue queue(0);

    const int producer_count = 2;
    const int consumer_count = 2;
    const int messages_per_producer = 5;
    const int messages_per_consumer = 5;

    std::vector<std::thread> threads;

    for (int i = 1; i <= consumer_count; ++i) {
        threads.emplace_back(consumer, std::ref(queue), i, messages_per_consumer);
    }

    for (int i = 1; i <= producer_count; ++i) {
        threads.emplace_back(producer, std::ref(queue), i, messages_per_producer);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    std::string message;
    if (!queue.nb_msg_get(message)) {
        std::cout << "Non-blocking get: queue is empty\n";
    }

    QueueStats info = queue.stats();
    std::cout << "\nQueue statistics:\n";
    std::cout << "Messages in queue: " << info.message_count << '\n';
    std::cout << "Total message size: " << info.total_message_size << " bytes\n";
    std::cout << "Process memory usage: " << info.memory_usage << " bytes\n";

    return 0;
}
