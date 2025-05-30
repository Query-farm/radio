#pragma once
#include "radio_extension.hpp"
#include "radio_transmit_message.hpp"
#include "radio_utils.hpp"
#include <algorithm>

namespace duckdb {

class RadioTransmitMessage;
struct RadioTransmitMessageQueueState {
	uint64_t odometer = 0;

	uint64_t dropped_unprocessed = 0;

	uint64_t latest_queue_time = 0;

	uint64_t latest_success_time = 0;
	uint64_t latest_failure_time = 0;

	// Total number of messages successfully transmitted
	uint64_t transmit_successes = 0;

	// Number of attempts to transmit that failed.
	uint64_t transmit_failures = 0;
};

class RadioTransmitMessageQueue {
public:
	RadioTransmitMessageQueue(const size_t capacity, const int32_t retry_initial_delay_ms,
	                          const double retry_multiplier, const int32_t retry_max_delay_ms)
	    : capacity_(capacity), retry_initial_delay_ms_(retry_initial_delay_ms), retry_multiplier_(retry_multiplier),
	      retry_max_delay_ms_(retry_max_delay_ms) {
	}

	void push(const std::vector<std::shared_ptr<RadioTransmitMessage>> &items, const uint64_t current_time);

	std::shared_ptr<RadioTransmitMessage> pop(uint64_t current_time);
	std::vector<std::shared_ptr<RadioTransmitMessage>> snapshot() const;

	void resize(uint64_t new_capacity);

	void remove_by_ids(const std::unordered_set<uint64_t> &ids_to_remove);

	uint64_t size() const;

	uint64_t capacity() const;

	uint64_t dropped_unprocessed() const;

	bool empty() const;

	void clear();

	void record_transmit_attempt(bool success, uint64_t last_attempt_time);

	int32_t retry_delay_time_ms(const RadioTransmitMessage &message) const;

	RadioTransmitMessageQueueState state() const;

private:
	uint64_t capacity_;

	int32_t retry_initial_delay_ms_;
	double retry_multiplier_;
	int32_t retry_max_delay_ms_;

	std::deque<std::shared_ptr<RadioTransmitMessage>> queue_;

	RadioTransmitMessageQueueState state_;

	mutable std::mutex mtx;
};

} // namespace duckdb