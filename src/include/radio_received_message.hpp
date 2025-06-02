#pragma once
#include "duckdb.hpp"

namespace duckdb {

class RadioSubscription;

class RadioReceivedMessage {
public:
	enum class MessageType { Message, Error, Connection, Disconnection };

	static MessageType convert_to_message_type(const std::string &type) {
		static const std::unordered_map<std::string, MessageType> map = {{"message", MessageType::Message},
		                                                                 {"error", MessageType::Error},
		                                                                 {"connection", MessageType::Connection},
		                                                                 {"disconnection", MessageType::Disconnection}};
		auto it = map.find(type);
		if (it == map.end()) {
			throw InvalidInputException("Invalid message type: " + type);
		}
		return it->second;
	}

	static uint16_t message_type_to_enum_index(const MessageType type) {
		return static_cast<uint16_t>(type);
	}

	explicit RadioReceivedMessage(RadioSubscription &subscription, const uint64_t id, const MessageType type,
	                              const std::optional<std::string> channel, const std::string &message,
	                              const uint64_t receive_time)
	    : subscription_(subscription), id_(id), type_(type), channel_(std::move(channel)), message_(std::move(message)),
	      receive_time_(receive_time) {
	}

	void increment_seen_count() {
		seen_count_.fetch_add(1);
	}

	[[nodiscard]] MessageType type() const {
		return type_;
	}

	[[nodiscard]] const std::optional<std::string> &channel() const {
		return channel_;
	}

	[[nodiscard]] const std::string &message() const {
		return message_;
	}

	[[nodiscard]] uint64_t receive_time() const {
		return receive_time_;
	}

	[[nodiscard]] uint64_t seen_count() const {
		return seen_count_.load();
	}

	[[nodiscard]] uint64_t id() const {
		return id_;
	}

	[[nodiscard]] RadioSubscription &subscription() const {
		return subscription_;
	}

private:
	// Store the ID of the message, it never changes and is relative to the subscription.
	RadioSubscription &subscription_;
	const uint64_t id_;

	const MessageType type_;

	const std::optional<std::string> channel_;
	const std::string message_;
	const uint64_t receive_time_;

	// The number of times this message has been seen.
	std::atomic<uint64_t> seen_count_ {0};
};

} // namespace duckdb