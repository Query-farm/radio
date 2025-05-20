#pragma once
#include "duckdb.hpp"

namespace duckdb {

class RadioReceivedMessage {
public:
	enum MessageType { MESSAGE, ERROR };
	explicit RadioReceivedMessage(const uint64_t id, const std::string &message, const uint64_t receive_time)
	    : id_(id), message_(std::move(message)), receive_time_(receive_time) {
	}

	void increment_seen_count() {
		seen_count_.fetch_add(1);
	}

	const std::string &message() const {
		return message_;
	}

	uint64_t receive_time() const {
		return receive_time_;
	}

	uint64_t get_seen_count() const {
		return seen_count_.load();
	}

	uint64_t get_id() const {
		return id_;
	}

private:
	// Store the ID of the message, it never changes and is relative to the subscription.
	const uint64_t id_;

	const std::string message_;
	const uint64_t receive_time_;

	// The number of times this message has been seen.
	std::atomic<uint64_t> seen_count_ {0};
};

}