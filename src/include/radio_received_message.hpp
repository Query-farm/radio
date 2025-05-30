#pragma once
#include "duckdb.hpp"

namespace duckdb {

class RadioSubscription;

class RadioReceivedMessage {
public:
	enum MessageType { MESSAGE, ERROR };
	explicit RadioReceivedMessage(RadioSubscription &subscription, const uint64_t id, MessageType &type,
	                              const std::string &message, const uint64_t receive_time)
	    : subscription_(subscription), id_(id), type_(type), message_(std::move(message)), receive_time_(receive_time) {
	}

	void increment_seen_count() {
		seen_count_.fetch_add(1);
	}

	const MessageType type() const {
		return type_;
	}

	const std::string &message() const {
		return message_;
	}

	uint64_t receive_time() const {
		return receive_time_;
	}

	uint64_t seen_count() const {
		return seen_count_.load();
	}

	uint64_t id() const {
		return id_;
	}

	RadioSubscription &subscription() const {
		return subscription_;
	}

private:
	// Store the ID of the message, it never changes and is relative to the subscription.
	RadioSubscription &subscription_;
	const uint64_t id_;

	const MessageType type_;

	const std::string message_;
	const uint64_t receive_time_;

	// The number of times this message has been seen.
	std::atomic<uint64_t> seen_count_ {0};
};

} // namespace duckdb