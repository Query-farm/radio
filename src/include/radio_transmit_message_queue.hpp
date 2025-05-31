#pragma once
#include "radio_extension.hpp"
#include "radio_transmit_message.hpp"
#include "radio_utils.hpp"
#include <algorithm>
#include <IXWebSocket.h>

namespace duckdb {

struct RadioTransmitMessageQueueState {
	uint64_t odometer = 0;

	uint64_t latest_queue_time = 0;

	uint64_t latest_success_time = 0;
	uint64_t latest_failure_time = 0;

	// Total number of messages successfully transmitted
	uint64_t successes = 0;

	// Number of attempts to transmit that failed.
	uint64_t failures = 0;
};

class RadioTransmitMessageQueue {
public:
	RadioTransmitMessageQueue(const int32_t retry_initial_delay_ms, const double retry_multiplier,
	                          const int32_t retry_max_delay_ms)
	    : retry_initial_delay_ms_(retry_initial_delay_ms), retry_multiplier_(retry_multiplier),
	      retry_max_delay_ms_(retry_max_delay_ms), stop_flag_(false) {
	}

	void push(const std::vector<std::shared_ptr<RadioTransmitMessage>> &items);

	std::shared_ptr<RadioTransmitMessage> wait_and_pop();

	//	std::shared_ptr<RadioTransmitMessage> pop_pending_send();
	std::vector<std::shared_ptr<RadioTransmitMessage>> snapshot() const;

	// Resive the capacity of the processed messages queue.
	// void resize(uint64_t new_capacity);

	void remove_by_ids(const std::unordered_set<uint64_t> &ids_to_remove);

	uint64_t size() const;

	// Return the capacity of the processed messages
	// uint64_t capacity() const;

	bool empty() const;

	void clear();

	std::condition_variable &pending_by_send_cv() {
		return pending_by_send_cv_;
	}

	void update_message_state(std::shared_ptr<RadioTransmitMessage> message,
	                          const RadioTransmitMessageProcessingState new_state, const uint64_t current_time,
	                          const std::string &transmit_result);

	RadioTransmitMessageQueueState state() const;

	void senderLoop(ix::WebSocket &websocket);

	void stop() {
		stop_flag_ = true;
		pending_by_send_cv_.notify_all();
	}

	void delete_finished();

private:
	void rebuild_pending_by_send_time();

	int32_t retry_initial_delay_ms_;
	double retry_multiplier_;
	int32_t retry_max_delay_ms_;

	struct CompareQueuedMessage {
		bool operator()(const std::shared_ptr<RadioTransmitMessage> &a,
		                const std::shared_ptr<RadioTransmitMessage> &b) const {
			return a->state().next_attempt_time > b->state().next_attempt_time; // Min-heap based on next attempt time
		}
	};

	std::priority_queue<std::shared_ptr<RadioTransmitMessage>, std::vector<std::shared_ptr<RadioTransmitMessage>>,
	                    CompareQueuedMessage>
	    pending_by_send_time_;
	std::condition_variable pending_by_send_cv_;

	std::unordered_map<uint64_t, std::shared_ptr<RadioTransmitMessage>> messages_by_id_;

	RadioTransmitMessageQueueState state_;

	mutable std::mutex mtx;
	std::atomic<bool> stop_flag_;
};

} // namespace duckdb