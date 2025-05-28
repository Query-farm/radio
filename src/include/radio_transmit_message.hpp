#pragma once
#include "duckdb.hpp"
#include "radio_transmit_message_queue.hpp"

namespace duckdb {

struct RadioTransmitMessageParts {
	std::string message;
	uint64_t expire_time;
	uint32_t max_attempts;
};

struct RadioTransmitMessageState {
	uint64_t last_transmit_time = 0;
	uint32_t try_count = 0;
	bool sent_ = false;
};

// Represent a message that is to be sent over a subscription.
// it has a body, the number of retries, and the time it was created.
class RadioTransmitMessage {
public:
	explicit RadioTransmitMessage(RadioTransmitMessageQueue &queue, const uint64_t id, const std::string &message,
	                              const uint64_t creation_time, const uint32_t max_attempts, const uint64_t expire_time)
	    : queue_(queue), id_(id), message_(std::move(message)), creation_time_(creation_time),
	      max_attempts_(max_attempts), expire_time_(expire_time) {
		D_ASSERT(max_attempts > 0);
		D_ASSERT(expire_time > creation_time);
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
		return !state_.sent_ && current_time < expire_time_ && state_.try_count < max_attempts_;
	}

	bool successfully_sent() const {
		std::lock_guard<std::mutex> lock(mtx);
		return state_.sent_;
	}

	bool was_processed(uint64_t current_time) const {
		std::lock_guard<std::mutex> lock(mtx);
		return state_.sent_ || state_.try_count >= max_attempts_ || creation_time_ >= expire_time_;
	}

	RadioTransmitMessageState state() const {
		std::lock_guard<std::mutex> lock(mtx);
		return state_;
	}

	// Store the result of the last transmission attempt.
	void store_result(bool success, uint64_t last_attempt_time) {
		{
			std::lock_guard<std::mutex> lock(mtx);
			D_ASSERT(state_.sent_ == false);
			state_.sent_ = success;
			state_.last_transmit_time = last_attempt_time;
			state_.try_count++;
		}

		queue_.record_transmit_attempt(success, last_attempt_time);
	}

	uint64_t id() const {
		return id_;
	}

	uint64_t expire_time() const {
		return expire_time_;
	}

	uint32_t max_attempts() const {
		return max_attempts_;
	}

private:
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

	RadioTransmitMessageState state_;
};

} // namespace duckdb