#include "../src/message_queue.h"

#include <iostream>
#include <string>

int main() {
    const size_t capacity = 3;
    MessageQueue queue(capacity);
    bool passed = true;

    std::cout << "Bounded queue test (capacity: " << capacity << ")\n\n";

    for (int i = 1; i <= 5; ++i) {
        std::string message = "Message " + std::to_string(i);
        bool accepted = queue.msg_send(message);
        bool expected = (i <= static_cast<int>(capacity));

        std::cout << message << ": " << (accepted ? "Accepted" : "Rejected")
                  << (accepted == expected ? " [OK]" : " [FAILED]") << '\n';
        if (accepted != expected) passed = false;
    }

    QueueStats info = queue.stats();
    std::cout << "\nQueue statistics:\n";
    std::cout << "Messages in queue: " << info.message_count << '\n';
    std::cout << "Total message size: " << info.total_message_size << " bytes\n";
    std::cout << "Process memory usage: " << info.memory_usage << " bytes\n";

    if (info.message_count != capacity || info.total_message_size != 27)
        passed = false;

    std::cout << "\nTest result: " << (passed ? "PASSED" : "FAILED") << '\n';
    return passed ? 0 : 1;
}
