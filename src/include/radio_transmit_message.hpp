#pragma once
#include "duckdb.hpp"
#include "radio_transmit_message_queue.hpp"

namespace duckdb {

// Represent a message that is to be sent over a subscription.
// it has a body, the number of retries, and the time it was created.
class RadioTransmitMessage {
public:
	explicit RadioTransmitMessage(RadioTransmitMessageQueue &queue, const uint64_t id, const std::string &message,
	                              const uint64_t creation_time, const uint32_t max_attempts, const uint64_t expire_time)
	    : queue_(queue), id_(id), message_(std::move(message)), creation_time_(creation_time),
	      max_attempts_(max_attempts), expire_time_(expire_time) {
		D_ASSERT(max_attempts > 0);
	}

	const std::string &message() const {
		return message_;
	}

	uint64_t creation_time() const {
		return creation_time_;
	}

	// Indicate if the message should be sent.
	bool should_send(uint64_t current_time) const {
		std::lock_guard<std::mutex> lock(mtx);
		return !sent_ && current_time < expire_time_ && try_count_ < max_attempts_;
	}

	uint64_t transmit_last_attempt_time() const {
		std::lock_guard<std::mutex> lock(mtx);
		return last_transmit_time_;
	}

	bool successfully_sent() const {
		std::lock_guard<std::mutex> lock(mtx);
		return sent_;
	}

	bool was_processed(uint64_t current_time) const {
		std::lock_guard<std::mutex> lock(mtx);
		return sent_ || try_count_ >= max_attempts_ || creation_time_ >= expire_time_;
	}

	// Store the result of the last transmission attempt.
	void store_result(bool success, uint64_t last_attempt_time) {
		{
			std::lock_guard<std::mutex> lock(mtx);
			D_ASSERT(sent_ == false);
			sent_ = success;
			last_transmit_time_ = last_attempt_time;
			try_count_++;
		}

		queue_.record_transmit_attempt(success, last_attempt_time);
	}

	uint64_t transmit_try_count() const {
		std::lock_guard<std::mutex> lock(mtx);

		return try_count_;
	}

	uint64_t id() const {
		return id_;
	}

private:
	// Need to protect the subscriptions map.
	mutable std::mutex mtx;

	// The queue this message belongs to, because we need to update
	// stats when this message is sent.
	RadioTransmitMessageQueue &queue_;

	// Store the ID of the message, it never changes and is relative to the subscription.
	const uint64_t id_;

	const std::string message_;
	const uint64_t creation_time_;
	const uint32_t max_attempts_;

	// The time at which the message is expired and will no longer be sent,
	// if it hasn't previously been sent.
	const uint64_t expire_time_;

	// These members are all protected by the mutex.
	// Indicate if the message has been sent.
	bool sent_ = false;

	uint64_t last_transmit_time_ = 0;
	// The number of times this message has been seen.
	uint64_t try_count_ = 0;
};

} // namespace duckdb