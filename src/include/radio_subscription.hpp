#pragma once
#include "radio_extension.hpp"
#include "radio_received_message.hpp"
#include "radio_received_message_queue.hpp"
#include "radio_transmit_message.hpp"
#include "radio_transmit_message_queue.hpp"
#include "radio.hpp"
#include "radio_subscription_parameters.hpp"
#include <IXWebSocket.h>
#include "redis_subscription.hpp"
namespace duckdb {

class Radio;

struct RadioSubscriptionQueueState {
	RadioReceivedMessageQueueState received;
	RadioTransmitMessageQueueState transmit;
};

struct RadioReceiveMessageParts {
	RadioReceivedMessage::MessageType type;
	std::optional<std::string> channel;
	std::string message;
	uint64_t receive_time;
};

class RadioSubscription {
private:
	enum class UrlType { WebSocket, Redis, Unknown };
	static UrlType detect_url_type(const std::string &url);
	static std::string normalize_redis_url(const std::string &url);

public:
	explicit RadioSubscription(const uint64_t id, const std::string &url, const RadioSubscriptionParameters &params,
	                           uint64_t creation_time, Radio &radio);

	void start(std::shared_ptr<RadioSubscription> &self);
	void stop();

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

	uint64_t receive_has_unseen() {
		return received_messages_.has_unseen();
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

	bool has_unseen() const {
		return received_messages_.has_unseen();
	}

	void set_disabled(bool disabled) {
		// if (disabled) {
		// 	if (std::holds_alternative<std::unique_ptr<ix::WebSocket>>(connection_)) {
		// 		auto &webSocket = std::get<std::unique_ptr<ix::WebSocket>>(connection_);
		// 		webSocket->stop();
		// 	} else if (std::holds_alternative<RedisSubscription>(connection_)) {
		// 		auto &redis = std::get<RedisSubscription>(connection_);
		// 		redis.subscriber->unsubscribe();
		// 	}
		// } else {

		// 	// if (std::holds_alternative<std::unique_ptr<ix::WebSocket>>(connection_)) {
		// 	// 	auto &webSocket = std::get<std::unique_ptr<ix::WebSocket>>(connection_);
		// 	// 	webSocket->start();
		// 	// } else if (std::holds_alternative<RedisSubscription>(connection_)) {
		// 	// 	auto &redis = std::get<RedisSubscription>(connection_);
		// 	// 	D_ASSERT(channel_name_.has_value());
		// 	// 	redis.subscriber->subscribe(channel_name_.value());

		// 	// 	if (reader_thread_->joinable()) {
		// 	// 		reader_thread_->join();
		// 	// 	}

		// 	// 	reader_thread_ = std::thread([&redis] { redis.subscriber->consume(); });
		// 	// }
		// }
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

	std::variant<std::unique_ptr<ix::WebSocket>, RedisSubscription> connection;

	std::atomic<uint64_t> activation_time_ {0};

private:
	// Store the ID of the subscription, it never changes.
	const uint64_t id_;

	const UrlType url_type_;

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

	// Indicate if this subscription should be disabled.
	bool disabled_ = false;

	// Keep a queue of messages here, so its easier to manage rather than a shared queue.
	RadioReceivedMessageQueue received_messages_;

	RadioTransmitMessageQueue transmit_messages_;

	Radio &radio_;

	bool is_stopped_ = false;
};

void RadioSubscriptionAddFunctions(DatabaseInstance &instance);

} // namespace duckdb