#include "../src/message_queue.h"

#include <iostream>
#include <string>
#include <vector>

int main() {
    MessageQueue queue;
    const std::vector<std::string> messages = {
        "Message 1", "Message 2", "Message 3", "Message 4", "Message 5"
    };

    std::cout << "FIFO queue test\n\n";
    bool passed = true;

    for (const auto& message : messages) {
        if (!queue.msg_send(message)) {
            std::cout << "Failed to send: " << message << '\n';
            passed = false;
        } else {
            std::cout << "Sent: " << message << '\n';
        }
    }

    std::cout << "\n";
    for (const auto& expected : messages) {
        std::string received;
        if (!queue.nb_msg_get(received)) {
            std::cout << "Expected: " << expected << ", but queue was empty [FAIL]\n";
            passed = false;
            continue;
        }
        bool matches = received == expected;
        std::cout << "Expected: " << expected << " | Received: " << received
                  << (matches ? " [OK]" : " [FAIL]") << '\n';
        if (!matches) passed = false;
    }

    std::string extra;
    if (queue.nb_msg_get(extra)) {
        std::cout << "Unexpected extra message: " << extra << '\n';
        passed = false;
    }

    QueueStats stats = queue.stats();
    if (stats.message_count != 0 || stats.total_message_size != 0) {
        passed = false;
    }

    std::cout << "\nMessages left in queue: " << stats.message_count << '\n';
    std::cout << "Total message size: " << stats.total_message_size << " bytes\n";
    std::cout << "\nTest result: " << (passed ? "PASSED" : "FAILED") << '\n';
    return passed ? 0 : 1;
}
