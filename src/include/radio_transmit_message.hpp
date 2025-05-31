#pragma once
#include "duckdb.hpp"

namespace duckdb {

struct RadioTransmitMessageParts {
	std::string message;
	uint64_t expire_time;
	uint32_t max_attempts;
};

// The state of the message, proceeds PENDING, SENDING, SENT.
enum RadioTransmitMessageProcessingState { PENDING, SENDING, SENT, TIME_EXPIRED, RETRIES_EXHAUSTED };

struct RadioTransmitMessageState {
	RadioTransmitMessageProcessingState state = PENDING;
	uint64_t last_attempt_start_time = 0;
	uint64_t last_attempt_end_time = 0;

	// The time at which the next attempt to send the message will be made,
	// be default it will be sent right away.
	uint64_t next_attempt_time = 0;

	uint32_t attempts_made = 0;

	// Any data returned after sending the message.
	std::string transmit_result;
};

// Represent a message that is to be sent over a subscription.
// it has a body, the number of retries, and the time it was created.
class RadioTransmitMessage {
public:
	explicit RadioTransmitMessage(const uint64_t id, const std::string &message, const uint64_t creation_time,
	                              const uint32_t max_attempts, const uint64_t expire_time)
	    : id_(id), message_(std::move(message)), creation_time_(creation_time), max_attempts_(max_attempts),
	      send_expire_time_(expire_time) {
		D_ASSERT(max_attempts > 0);
		D_ASSERT(expire_time > creation_time);
	}

	const std::string &message() const {
		return message_;
	}

	uint64_t creation_time() const {
		return creation_time_;
	}

	RadioTransmitMessageState state() const {
		std::lock_guard<std::mutex> lock(mtx);
		return state_;
	}

	// Store the result of the last transmission attempt.
	void update_state(const RadioTransmitMessageProcessingState new_state, const uint64_t current_time,
	                  const int32_t retry_initial_delay_ms, const double retry_multiplier,
	                  const int32_t retry_max_delay_ms, const std::string &transmit_result) {
		std::lock_guard<std::mutex> lock(mtx);

		using State = RadioTransmitMessageProcessingState;
		D_ASSERT(new_state == State::PENDING || new_state == State::SENDING || new_state == State::SENT);

		auto &s_ = state_;
		if (new_state == State::PENDING) {
			D_ASSERT(s_.state == State::SENDING);
			// We were sending, but it didn't work.
			s_.state = new_state;
			s_.transmit_result = transmit_result;
			s_.last_attempt_end_time = current_time;

			if (s_.attempts_made >= max_attempts_) {
				s_.state = State::RETRIES_EXHAUSTED;
				s_.next_attempt_time = 0;
			} else {
				const auto delay = std::min(
				    static_cast<int32_t>(retry_initial_delay_ms * std::pow(retry_multiplier, s_.attempts_made)),
				    retry_max_delay_ms);

				const auto next_attempt_time = current_time + delay;
				if (next_attempt_time > send_expire_time_) {
					// If the next attempt time is after the expiration, we should not retry.
					s_.state = State::TIME_EXPIRED;
					s_.next_attempt_time = 0;
				} else {
					s_.next_attempt_time = next_attempt_time;
				}
			}
		} else if (new_state == State::SENT) {
			D_ASSERT(s_.state == State::SENDING);
			s_.state = new_state;
			s_.last_attempt_end_time = current_time;
			s_.transmit_result = transmit_result;
		} else if (new_state == State::SENDING) {
			D_ASSERT(s_.state == State::PENDING);
			s_.state = new_state;
			s_.last_attempt_start_time = current_time;
			s_.last_attempt_end_time = 0;
			s_.transmit_result.clear();
			s_.next_attempt_time = 0;
			s_.attempts_made++;
		}
	}

	uint64_t id() const {
		return id_;
	}

	uint64_t send_expire_time() const {
		return send_expire_time_;
	}

	uint32_t max_attempts() const {
		return max_attempts_;
	}

private:
	mutable std::mutex mtx;

	// Store the ID of the message, it never changes and is relative to the subscription.
	const uint64_t id_;

	const std::string message_;
	const uint64_t creation_time_;
	const uint32_t max_attempts_;

	// The time at which the message is expired and will no longer be sent,
	// if it hasn't previously been sent.
	const uint64_t send_expire_time_;

	RadioTransmitMessageState state_;
};

} // namespace duckdb