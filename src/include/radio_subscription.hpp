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

	ix::WebSocket webSocket;

public:
	explicit RadioSubscription(const uint64_t id, const std::string &url, const RadioSubscriptionParameters &params,
	                           uint64_t creation_time, Radio &radio)
	    : id_(id), url_(std::move(url)), creation_time_(creation_time), disabled_(false),
	      received_messages_(params.receive_message_capacity), received_errors_(params.receive_error_capacity),
	      transmit_messages_(params.transmit_message_capacity, params.transmit_retry_initial_delay_ms,
	                         params.transmit_retry_multiplier, params.transmit_retry_max_delay_ms),
	      radio_(radio) {
		webSocket.setUrl(url_);
		webSocket.setOnMessageCallback([this](const ix::WebSocketMessagePtr &msg) {
			if (msg->type == ix::WebSocketMessageType::Message) {
				this->add_received_messages(RadioReceivedMessage::MessageType::MESSAGE,
				                            {{msg->str, RadioCurrentTimeMillis()}});
			} else if (msg->type == ix::WebSocketMessageType::Open) {
				this->activation_time_ = RadioCurrentTimeMillis();
			} else if (msg->type == ix::WebSocketMessageType::Close) {
				this->activation_time_ = 0;
			} else if (msg->type == ix::WebSocketMessageType::Error) {
				this->add_received_messages(RadioReceivedMessage::MessageType::ERROR,
				                            {{msg->errorInfo.reason, RadioCurrentTimeMillis()}});
			}
		});

		// FIXME: need a way to handle transmit messages in a seperate thread.

		webSocket.start();
	}

	~RadioSubscription() {
		webSocket.stop();
	}

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
	                           std::vector<std::pair<std::string, uint64_t>> messages,
	                           uint64_t *message_ids = nullptr) {
		std::vector<std::shared_ptr<RadioReceivedMessage>> items;

		for (size_t i = 0; i < messages.size(); i++) {
			auto message_id = message_counter_.fetch_add(1, std::memory_order_relaxed);
			if (message_ids) {
				message_ids[i] = message_id;
			}
			auto entry =
			    std::make_shared<RadioReceivedMessage>(*this, message_id, type, messages[i].first, messages[i].second);
			items.push_back(entry);
		}
		get_queue_for_type(type).push(items);

		radio_.NotifyHasMessages();
	}

	void add_transmit_messages(std::vector<RadioTransmitMessageParts> messages, uint64_t *message_ids) {
		std::vector<std::shared_ptr<RadioTransmitMessage>> items;

		auto current_time = RadioCurrentTimeMillis();
		for (size_t i = 0; i < messages.size(); i++) {
			auto message_id = message_counter_.fetch_add(1, std::memory_order_relaxed);
			if (message_ids) {
				message_ids[i] = message_id;
			}
			auto entry =
			    std::make_shared<RadioTransmitMessage>(this->transmit_messages_, message_id, messages[i].message,
			                                           current_time, messages[i].max_attempts, messages[i].expire_time);
			items.emplace_back(std::move(entry));
		}
		transmit_messages_.push(items, current_time);
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

	// Keep a queue of messages here, so its easier to manage rather than a shared queue.
	RadioReceivedMessageQueue received_messages_;

	// Store the latest error message.
	RadioReceivedMessageQueue received_errors_;

	RadioTransmitMessageQueue transmit_messages_;

	Radio &radio_;
};

void RadioSubscriptionAddFunctions(DatabaseInstance &instance);

} // namespace duckdb