#pragma once
#include "radio_extension.hpp"
#include "radio_received_message.hpp"
#include "radio_received_message_queue.hpp"
#include "radio.hpp"

namespace duckdb {

class Radio;
class RadioSubscription {

private:
	RadioReceivedMessageQueue &get_queue_for_type(const RadioReceivedMessage::MessageType type) {
		switch (type) {
		case RadioReceivedMessage::MESSAGE:
			return messages_;
		case RadioReceivedMessage::ERROR:
			return errors_;
		default:
			throw std::runtime_error("Unknown message type");
		}
	}

public:
	explicit RadioSubscription(const uint64_t id, const std::string &url, uint32_t max_queued_messages,
	                           uint64_t creation_time, Radio &radio)
	    : id_(id), url_(std::move(url)), creation_time_(creation_time), disabled_(false),
	      messages_(max_queued_messages), radio_(radio) {
	}

	void set_queue_size(RadioReceivedMessage::MessageType type, uint32_t max_queued_messages) {
		get_queue_for_type(type).resize(max_queued_messages);
	}

	std::vector<std::shared_ptr<RadioReceivedMessage>> snapshot_messages(RadioReceivedMessage::MessageType type) {
		return get_queue_for_type(type).snapshot();
	}

	uint64_t id() const {
		// Return the id of the subscription
		return id_;
	}

	const std::string &url() const {
		return url_;
	}

	uint64_t get_queue_size(RadioReceivedMessage::MessageType type) {
		return get_queue_for_type(type).size();
	}

	uint64_t creation_time() const {
		return creation_time_;
	}

	uint64_t activation_time() const {
		return activation_time_;
	}

	uint64_t get_latest_receive_time(RadioReceivedMessage::MessageType type) {
		return get_queue_for_type(type).latest_receive_time();
	}

	uint64_t message_odometer(RadioReceivedMessage::MessageType type) {
		return get_queue_for_type(type).get_odometer();
	}

	bool disabled() const {
		return disabled_;
	}

	void set_disabled(bool disabled) {
		disabled_ = disabled;
	}

	void clear(RadioReceivedMessage::MessageType type) {
		return get_queue_for_type(type).clear();
	}

	// This could be called by another thread.
	void add_messages(RadioReceivedMessage::MessageType type, vector<std::pair<std::string, uint64_t>> messages,
	                  uint64_t *message_ids) {
		std::vector<std::shared_ptr<RadioReceivedMessage>> items;

		for (size_t i = 0; i < messages.size(); i++) {
			auto message_id = message_counter_.fetch_add(1, std::memory_order_relaxed);
			if (message_ids) {
				message_ids[i] = message_id;
			}
			auto entry = std::make_shared<RadioReceivedMessage>(message_id, messages[i].first, messages[i].second);
			items.push_back(entry);
		}
		get_queue_for_type(type).push(items);

		radio_.NotifyHasMessages();
	}

private:
	// Store the ID of the subscription, it never changes.
	const uint64_t id_;

	// Store the URL of the subscription.
	const std::string url_;

	// The id of the next message to be received, will increment
	// every time a message is received
	//
	// Since this counter could be used from multiple threads it with an atomic.
	std::atomic<uint64_t> message_counter_ {1000};

	// Store the time of creation.
	const uint64_t creation_time_;

	// This state may need to be protected by a mutex.

	// The time the subscription was activated.
	uint64_t activation_time_ {0};

	// Indicate if this subscription should be disabled.
	bool disabled_ = false;

	// Store the latest error message.
	RadioReceivedMessageQueue errors_ {10};

	// Keep a queue of messages here, so its easier to manage rather than a shared queue.
	RadioReceivedMessageQueue messages_ {10};

	Radio &radio_;
};

void RadioSubscriptionAddFunctions(DatabaseInstance &instance);

} // namespace duckdb