#include "radio_received_message_queue.hpp"
#include "radio_utils.hpp"
#include "radio_subscription.hpp"
namespace duckdb {

void RadioReceivedMessageQueue::push(const std::vector<std::shared_ptr<RadioReceivedMessage>> &items) {
	std::lock_guard<std::mutex> lock(mtx);

	state_.odometer += items.size();
	for (auto &item : items) {
		if (queue_.size() >= capacity_) {
			auto &front = queue_.front();
			if (front->seen_count() == 0) {
				state_.dropped_unseen++;
			}
			queue_.pop_front(); // Drop oldest
		}
		state_.latest_receive_time =
		    item->receive_time() > state_.latest_receive_time ? item->receive_time() : state_.latest_receive_time;
		queue_.push_back(item);
	}
}

std::shared_ptr<RadioReceivedMessage> RadioReceivedMessageQueue::pop() {
	std::lock_guard<std::mutex> lock(mtx);
	if (queue_.empty()) {
		return nullptr;
	}
	auto item = queue_.front();
	if (item->seen_count() == 0) {
		state_.dropped_unseen++;
	}
	queue_.pop_front();
	return item;
}

std::vector<std::shared_ptr<RadioReceivedMessage>> RadioReceivedMessageQueue::snapshot() const {
	std::lock_guard<std::mutex> lock(mtx);
	for (const auto &item : queue_) {
		item->increment_seen_count();
	}
	return std::vector<std::shared_ptr<RadioReceivedMessage>>(queue_.begin(), queue_.end());
}

void RadioReceivedMessageQueue::resize(uint64_t new_capacity) {
	std::lock_guard<std::mutex> lock(mtx);
	capacity_ = new_capacity;
	while (queue_.size() > capacity_) {
		auto item = queue_.front();
		if (item->seen_count() == 0) {
			state_.dropped_unseen++;
		}
		queue_.pop_front();
	}
}

void RadioReceivedMessageQueue::remove_by_ids(const std::unordered_set<uint64_t> &ids_to_remove) {
	std::lock_guard<std::mutex> lock(mtx);
	queue_.erase(std::remove_if(queue_.begin(), queue_.end(),
	                            [&](const std::shared_ptr<RadioReceivedMessage> &item) {
		                            return ids_to_remove.count(item->id()) > 0;
	                            }),
	             queue_.end());
}

uint64_t RadioReceivedMessageQueue::size() const {
	std::lock_guard<std::mutex> lock(mtx);
	return queue_.size();
}

bool RadioReceivedMessageQueue::has_unseen() const {
	std::lock_guard<std::mutex> lock(mtx);
	for (const auto &item : queue_) {
		if (item->seen_count() == 0) {
			return true;
		}
	}
	return false;
}

uint64_t RadioReceivedMessageQueue::capacity() const {
	std::lock_guard<std::mutex> lock(mtx);
	return capacity_;
}

bool RadioReceivedMessageQueue::empty() const {
	std::lock_guard<std::mutex> lock(mtx);
	return queue_.empty();
}

void RadioReceivedMessageQueue::clear() {
	std::lock_guard<std::mutex> lock(mtx);
	queue_.clear();
}

void blackholeMessageHandler(const ix::WebSocketMessagePtr &msg) {
	// This is a blackhole handler that does nothing with the message.
	// It can be used to prevent the WebSocket from processing messages.
}

void RadioReceivedMessageQueue::stop() {
	stop_flag_ = true;
	if (std::holds_alternative<std::unique_ptr<ix::WebSocket>>(subscription_.connection)) {
		auto &websocket = std::get<std::unique_ptr<ix::WebSocket>>(subscription_.connection);
		if (websocket) {
			websocket->setOnMessageCallback(blackholeMessageHandler);
		}
	}

	if (reader_thread_.joinable()) {
		reader_thread_.join();
	}
}

RadioReceivedMessageQueueState RadioReceivedMessageQueue::state() const {
	std::lock_guard<std::mutex> lock(mtx);
	return state_;
}

void RadioReceivedMessageQueue::readerLoop() {
	try {
		if (!std::holds_alternative<RedisSubscription>(subscription_.connection)) {
			return;
		}
		auto &redis = std::get<RedisSubscription>(subscription_.connection);
		auto subscriber = redis.redis->subscriber();
		subscriber.on_message([this](std::string channel, std::string msg) {
			if (stop_flag_) {
				return;
			}
			const auto now = RadioCurrentTimeMillis();
			subscription_.add_received_messages({{RadioReceivedMessage::MessageType::Message, channel, msg, now}});
		});

		subscriber.subscribe(redis.channel_name);
		subscription_.activation_time_ = RadioCurrentTimeMillis();
		while (!stop_flag_) {
			try {
				subscriber.consume();
			} catch (const sw::redis::TimeoutError &) {
				// The finite socket timeout is the shutdown wake-up mechanism.
				continue;
			}
		}
	} catch (const std::exception &error) {
		if (!stop_flag_) {
			try {
				subscription_.add_received_messages(
				    {{RadioReceivedMessage::MessageType::Error, std::nullopt, error.what(), RadioCurrentTimeMillis()}});
			} catch (...) {
			}
		}
		subscription_.activation_time_ = 0;
	} catch (...) {
		subscription_.activation_time_ = 0;
	}
}

void RadioReceivedMessageQueue::start() {
	stop_flag_ = false;
	if (std::holds_alternative<std::unique_ptr<ix::WebSocket>>(subscription_.connection)) {
		auto &websocket = std::get<std::unique_ptr<ix::WebSocket>>(subscription_.connection);
		websocket->setOnMessageCallback([this](const ix::WebSocketMessagePtr &msg) {
			if (stop_flag_) {
				return;
			}
			const auto now = RadioCurrentTimeMillis();
			auto &subscription = this->subscription_;
			if (msg->type == ix::WebSocketMessageType::Message) {
				subscription.add_received_messages(
				    {{RadioReceivedMessage::MessageType::Message, std::nullopt, msg->str, now}});
			} else if (msg->type == ix::WebSocketMessageType::Open) {
				subscription.add_received_messages(
				    {{RadioReceivedMessage::MessageType::Connection, std::nullopt, "", now}});
				subscription.activation_time_ = now;
			} else if (msg->type == ix::WebSocketMessageType::Close) {
				subscription.add_received_messages(
				    {{RadioReceivedMessage::MessageType::Disconnection, std::nullopt, "", now}});
				subscription.activation_time_ = 0;
			} else if (msg->type == ix::WebSocketMessageType::Error) {
				subscription.add_received_messages(
				    {{RadioReceivedMessage::MessageType::Error, std::nullopt, msg->errorInfo.reason, now}});
			}
		});
	} else {
		reader_thread_ = std::thread([this] { this->readerLoop(); });
	}
}

} // namespace duckdb
