#include "../src/message_queue.h"

#include <atomic>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

int main() {
    const int producer_count = 10;
    const int consumer_count = 10;
    const int messages_per_producer = 1000;
    const int total_messages = producer_count * messages_per_producer;

    MessageQueue queue; // Unlimited queue
    std::vector<std::atomic<int>> received(total_messages);
    for (auto& count : received) {
        count.store(0);
    }

    std::atomic<int> sent_count(0);
    std::atomic<int> received_count(0);
    std::atomic<int> invalid_count(0);
    std::atomic<int> send_failures(0);

    std::vector<std::thread> consumers;
    for (int c = 0; c < consumer_count; ++c) {
        consumers.emplace_back([&]() {
            for (int j = 0; j < total_messages / consumer_count; ++j) {
                std::string message = queue.msg_get();
                try {
                    size_t parsed = 0;
                    int id = std::stoi(message, &parsed);
                    if (parsed == message.size() && id >= 0 && id < total_messages) {
                        received[id].fetch_add(1);
                    } else {
                        invalid_count.fetch_add(1);
                    }
                } catch (...) {
                    invalid_count.fetch_add(1);
                }
                received_count.fetch_add(1);
            }
        });
    }

    std::vector<std::thread> producers;
    for (int p = 0; p < producer_count; ++p) {
        producers.emplace_back([&, p]() {
            for (int j = 0; j < messages_per_producer; ++j) {
                int id = p * messages_per_producer + j;
                if (queue.msg_send(std::to_string(id))) {
                    sent_count.fetch_add(1);
                } else {
                    send_failures.fetch_add(1);
                }
            }
        });
    }

    for (auto& producer : producers) producer.join();
    for (auto& consumer : consumers) consumer.join();

    int missing = 0;
    int duplicates = 0;
    for (const auto& count : received) {
        int n = count.load();
        if (n == 0) ++missing;
        if (n > 1) duplicates += n - 1;
    }

    QueueStats stats = queue.stats();
    bool passed = sent_count == total_messages
               && received_count == total_messages
               && send_failures == 0
               && invalid_count == 0
               && missing == 0
               && duplicates == 0
               && stats.message_count == 0
               && stats.total_message_size == 0;

    std::cout << "Concurrency stress test\n\n";
    std::cout << "Producers: " << producer_count << '\n';
    std::cout << "Consumers: " << consumer_count << '\n';
    std::cout << "Expected messages: " << total_messages << '\n';
    std::cout << "Sent messages: " << sent_count << '\n';
    std::cout << "Received messages: " << received_count << '\n';
    std::cout << "Missing messages: " << missing << '\n';
    std::cout << "Duplicate messages: " << duplicates << '\n';
    std::cout << "Invalid messages: " << invalid_count << '\n';
    std::cout << "Send failures: " << send_failures << '\n';
    std::cout << "Messages left in queue: " << stats.message_count << '\n';
    std::cout << "Total message size: " << stats.total_message_size << " bytes\n";
    std::cout << "\nTest result: " << (passed ? "PASSED" : "FAILED") << '\n';
    return passed ? 0 : 1;
}
