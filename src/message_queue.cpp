#include "message_queue.h"

#ifdef _WIN32
#include <windows.h>
#include <psapi.h>
#else
#include <unistd.h>
#endif

#include <cerrno>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <utility>

MessageQueue::MessageQueue(size_t capacity)
    : head(new Node()), tail(head), max_size(capacity),
      message_count(0), total_message_size(0) {
    if (sem_init(&available_messages, 0, 0) != 0) {
        delete head;
        throw std::runtime_error("Could not initialize semaphore");
    }
}

// Destroy only after every producer and consumer thread has finished.
MessageQueue::~MessageQueue() {
    while (head != nullptr) {
        Node* old = head;
        head = old->next.load(std::memory_order_relaxed);
        delete old;
    }
    sem_destroy(&available_messages);
}

bool MessageQueue::msg_send(const std::string& message) {
    // Prepare the node before changing the queue or reserving capacity.
    std::unique_ptr<Node> new_node(new Node(message));

    // Reserve a slot. This includes in-progress sends in the capacity limit.
    {
        std::lock_guard<std::mutex> lock(stats_mutex);
        if (max_size != 0 && message_count >= max_size)
            return false;
        ++message_count;
        total_message_size += message.size();
    }

    // Producers only lock the tail; consumers only lock the head.
    {
        std::lock_guard<std::mutex> lock(tail_mutex);
        tail->next.store(new_node.get(), std::memory_order_release);
        tail = new_node.release();
    }

    // A consumer may now claim this fully published message.
    sem_post(&available_messages);
    return true;
}

bool MessageQueue::remove_message(std::string& message) {
    std::lock_guard<std::mutex> head_lock(head_mutex);
    Node* next = head->next.load(std::memory_order_acquire);
    if (next == nullptr)
        return false; // Defensive: should not happen after a semaphore claim.

    // The next node becomes the dummy head after its message is taken.
    // swap avoids an allocation or exception while changing the head.
    message.swap(next->message);
    Node* old_head = head;
    head = next;
    delete old_head;

    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex);
        --message_count;
        total_message_size -= message.size();
    }
    return true;
}

std::string MessageQueue::msg_get() {
    while (sem_wait(&available_messages) == -1) {
        if (errno != EINTR)
            throw std::runtime_error("Semaphore wait failed");
    }

    std::string message;
    if (!remove_message(message))
        throw std::runtime_error("Queue state is inconsistent");
    return message;
}

bool MessageQueue::nb_msg_get(std::string& message) {
    if (sem_trywait(&available_messages) == -1) {
        if (errno == EAGAIN)
            return false;
        throw std::runtime_error("Semaphore trywait failed");
    }
    if (!remove_message(message))
        throw std::runtime_error("Queue state is inconsistent");
    return true;
}

QueueStats MessageQueue::stats() {
    QueueStats result;
    {
        std::lock_guard<std::mutex> lock(stats_mutex);
        result.message_count = message_count;
        result.total_message_size = total_message_size;
    }
    result.memory_usage = get_process_memory();
    return result;
}

size_t MessageQueue::get_process_memory() const {
#ifdef _WIN32
    PROCESS_MEMORY_COUNTERS info;
    if (GetProcessMemoryInfo(GetCurrentProcess(), &info, sizeof(info)))
        return static_cast<size_t>(info.WorkingSetSize);
    return 0;
#else
    FILE* file = fopen("/proc/self/statm", "r");
    if (!file) return 0;
    long total_pages = 0, resident_pages = 0;
    int count = fscanf(file, "%ld %ld", &total_pages, &resident_pages);
    fclose(file);
    if (count != 2) return 0;
    long page_size = sysconf(_SC_PAGESIZE);
    return page_size > 0 ? static_cast<size_t>(resident_pages) *
                               static_cast<size_t>(page_size) : 0;
#endif
}
