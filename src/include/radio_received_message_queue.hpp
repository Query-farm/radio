#pragma once
#include "radio_extension.hpp"
#include "radio_received_message.hpp"
#include "redis_subscription.hpp"
#include <IXWebSocket.h>
#include <iostream>

namespace duckdb {

struct RadioReceivedMessageQueueState {
	uint64_t odometer = 0;

	uint64_t dropped_unseen = 0;

	uint64_t latest_receive_time = 0;
};

class RadioReceivedMessageQueue {
public:
	RadioReceivedMessageQueue(RadioSubscription &subscription, size_t capacity)
	    : subscription_(subscription), capacity_(capacity) {
	}

	void push(const std::vector<std::shared_ptr<RadioReceivedMessage>> &items);

	std::shared_ptr<RadioReceivedMessage> pop();

	std::vector<std::shared_ptr<RadioReceivedMessage>> snapshot() const;

	void resize(uint64_t new_capacity);

	void remove_by_ids(const std::unordered_set<uint64_t> &ids_to_remove);

	uint64_t size() const;

	bool has_unseen() const;

	uint64_t capacity() const;

	bool empty() const;

	void clear();

	void stop();

	void start();

	RadioReceivedMessageQueueState state() const;

	void readerLoop();

private:
	atomic<bool> stop_flag_ {false};
	RadioSubscription &subscription_;

	uint64_t capacity_;

	RadioReceivedMessageQueueState state_;

	std::deque<std::shared_ptr<RadioReceivedMessage>> queue_;

	mutable std::mutex mtx;
	std::thread reader_thread_;
};
} // namespace duckdb