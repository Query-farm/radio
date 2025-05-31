#pragma once
#include "radio_extension.hpp"
#include "radio_received_message.hpp"
#include "radio_received_message_queue.hpp"
#include "radio_transmit_message.hpp"
#include "radio_transmit_message_queue.hpp"
#include "radio.hpp"
#include "radio_subscription_parameters.hpp"
#include <IXWebSocket.h>

namespace duckdb {

class Radio;

struct RadioSubscriptionQueueState {
	RadioReceivedMessageQueueState received_messages;
	RadioReceivedMessageQueueState received_errors;
	RadioTransmitMessageQueueState transmit;
};

class RadioSubscription {

private:
	RadioReceivedMessageQueue &get_queue_for_type(const RadioReceivedMessage::MessageType type) {
		switch (type) {
		case RadioReceivedMessage::MESSAGE:
			return received_messages_;
		case RadioReceivedMessage::ERROR:
			return received_errors_;
		default:
			throw std::runtime_error("Unknown message type");
		}
	}

public:
	explicit RadioSubscription(const uint64_t id, const std::string &url, const RadioSubscriptionParameters &params,
	                           uint64_t creation_time, Radio &radio);

	~RadioSubscription();

	void set_receive_queue_size(RadioReceivedMessage::MessageType type, uint32_t capacity) {
		get_queue_for_type(type).resize(capacity);
	}

	std::vector<std::shared_ptr<RadioReceivedMessage>> receive_snapshot(RadioReceivedMessage::MessageType type) {
		return get_queue_for_type(type).snapshot();
	}

	std::vector<std::shared_ptr<RadioTransmitMessage>> transmit_snapshot() {
		return transmit_messages_.snapshot();
	}

	uint64_t id() const {
		// Return the id of the subscription
		return id_;
	}

	const std::string &url() const {
		return url_;
	}

	uint64_t receive_queue_size(RadioReceivedMessage::MessageType type) {
		return get_queue_for_type(type).size();
	}

	uint64_t creation_time() const {
		return creation_time_;
	}

	uint64_t activation_time() const {
		return activation_time_;
	}

	RadioSubscriptionQueueState state() {
		RadioSubscriptionQueueState state;
		state.received_messages = get_queue_for_type(RadioReceivedMessage::MESSAGE).state();
		state.received_errors = get_queue_for_type(RadioReceivedMessage::ERROR).state();
		state.transmit = transmit_messages_.state();
		return state;
	}

	bool disabled() const {
		return disabled_;
	}

	void set_disabled(bool disabled) {
		if (disabled) {
			webSocket.stop();
		} else {
			webSocket.start();
		}
		disabled_ = disabled;
	}

	void clear(RadioReceivedMessage::MessageType type) {
		return get_queue_for_type(type).clear();
	}

	// This could be called by another thread.
	void add_received_messages(RadioReceivedMessage::MessageType type,
	                           std::vector<std::pair<std::string, uint64_t>> messages, uint64_t *message_ids = nullptr);

	void add_transmit_messages(std::vector<RadioTransmitMessageParts> messages, uint64_t *message_ids);

	void transmit_messages_delete_finished() {
		transmit_messages_.delete_finished();
	}

	void transmit_messages_delete_by_ids(const std::unordered_set<uint64_t> &ids_to_remove) {
		transmit_messages_.delete_by_ids(ids_to_remove);
	}

private:
	void senderLoop();

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

	// Keep a queue of messages here, so its easier to manage rather than a shared queue.
	RadioReceivedMessageQueue received_messages_;

	// Store the latest error message.
	RadioReceivedMessageQueue received_errors_;

	RadioTransmitMessageQueue transmit_messages_;

	Radio &radio_;

	ix::WebSocket webSocket;

	std::thread sender_thread_;
};

void RadioSubscriptionAddFunctions(DatabaseInstance &instance);

} // namespace duckdb