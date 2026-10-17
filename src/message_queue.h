#ifndef MESSAGE_QUEUE_H
#define MESSAGE_QUEUE_H

#include <atomic>
#include <cstddef>
#include <mutex>
#include <semaphore.h>
#include <string>

struct QueueStats {
    size_t message_count;
    size_t total_message_size;
    size_t memory_usage;
};

class MessageQueue {
private:
    struct Node {
        std::string message;
        std::atomic<Node*> next;

        explicit Node(const std::string& value = "")
            : message(value), next(nullptr) {}
    };

    Node* head;
    Node* tail;
    const size_t max_size;
    size_t message_count;
    size_t total_message_size;

    std::mutex head_mutex;
    std::mutex tail_mutex;
    std::mutex stats_mutex;
    sem_t available_messages;

    bool remove_message(std::string& message);
    size_t get_process_memory() const;

public:
    // max_size = 0 means unlimited capacity.
    explicit MessageQueue(size_t max_size = 0);
    ~MessageQueue();

    MessageQueue(const MessageQueue&) = delete;
    MessageQueue& operator=(const MessageQueue&) = delete;

    bool msg_send(const std::string& message);
    std::string msg_get();
    bool nb_msg_get(std::string& message);
    QueueStats stats();
};

#endif
