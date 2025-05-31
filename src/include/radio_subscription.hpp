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
	RadioReceivedMessageQueueState received;
	RadioTransmitMessageQueueState transmit;
};

struct RadioReceiveMessageParts {
	RadioReceivedMessage::MessageType type;
	std::string message;
	uint64_t receive_time;
};

class RadioSubscription {

public:
	explicit RadioSubscription(const uint64_t id, const std::string &url, const RadioSubscriptionParameters &params,
	                           uint64_t creation_time, Radio &radio);

	~RadioSubscription();

	void set_receive_queue_size(uint32_t capacity) {
		received_messages_.resize(capacity);
	}

	std::vector<std::shared_ptr<RadioReceivedMessage>> receive_snapshot() {
		return received_messages_.snapshot();
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

	uint64_t receive_queue_size() {
		return received_messages_.size();
	}

	uint64_t creation_time() const {
		return creation_time_;
	}

	uint64_t activation_time() const {
		return activation_time_;
	}

	RadioSubscriptionQueueState state() {
		RadioSubscriptionQueueState state;
		state.received = received_messages_.state();
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

	void clear_received() {
		return received_messages_.clear();
	}

	// This could be called by another thread.
	void add_received_messages(std::vector<RadioReceiveMessageParts> messages, uint64_t *message_ids = nullptr);

	void add_transmit_messages(std::vector<RadioTransmitMessageParts> messages, uint64_t *message_ids);

	void transmit_messages_delete_finished() {
		transmit_messages_.delete_finished();
	}

	void transmit_messages_delete_by_ids(const std::unordered_set<uint64_t> &ids_to_remove) {
		transmit_messages_.delete_by_ids(ids_to_remove);
	}

	bool flush_complete(const std::chrono::steady_clock::time_point &timeout) {
		return transmit_messages_.flush_complete(timeout);
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

	RadioTransmitMessageQueue transmit_messages_;

	Radio &radio_;

	ix::WebSocket webSocket;

	std::thread sender_thread_;
};

void RadioSubscriptionAddFunctions(DatabaseInstance &instance);

} // namespace duckdb