#include "radio_transmit_message.hpp"
#include "radio_transmit_message_queue.hpp"
#include "radio_subscription.hpp"
#include <sw/redis++/redis++.h>

namespace duckdb {

void RadioTransmitMessageQueue::rebuild_pending_retry_queue() {
	pending_by_send_time_ = {};
	for (const auto &[_, msg] : messages_by_id_) {
		if (msg->state().state == RadioTransmitMessageProcessingState::PENDING) {
			pending_by_send_time_.push(msg);
		}
	}
	pending_by_send_cv_.notify_all();
}

void RadioTransmitMessageQueue::push(const std::vector<std::shared_ptr<RadioTransmitMessage>> &items) {
	std::lock_guard<std::mutex> lock(mtx);

	state_.odometer += items.size();
	for (auto &item : items) {
		pending_by_send_time_.push(item);
		messages_by_id_[item->id()] = item;
		state_.latest_queue_time = std::max(item->creation_time(), state_.latest_queue_time);
	}
	pending_by_send_cv_.notify_all();
}

void RadioTransmitMessageQueue::delete_finished() {
	std::lock_guard<std::mutex> lock(mtx);

	// Clear the pending messages where status == SENT
	for (auto it = messages_by_id_.begin(); it != messages_by_id_.end();) {
		if (it->second->state().state == RadioTransmitMessageProcessingState::SENT ||
		    it->second->state().state == RadioTransmitMessageProcessingState::TIME_EXPIRED ||
		    it->second->state().state == RadioTransmitMessageProcessingState::RETRIES_EXHAUSTED) {
			it = messages_by_id_.erase(it);
		} else {
			++it;
		}
	}

	rebuild_pending_retry_queue();
}

std::shared_ptr<RadioTransmitMessage> RadioTransmitMessageQueue::wait_and_pop() {
	std::unique_lock<std::mutex> lock(mtx);
	while (true) {
		if (stop_flag_) {
			return nullptr;
		}
		if (!pending_by_send_time_.empty()) {
			const auto now = std::chrono::steady_clock::now();
			const auto next_time = pending_by_send_time_.top()->state().next_attempt_time;
			if (!next_time.has_value()) {
				// If the next attempt time is not set, we can pop immediately.
				auto msg = pending_by_send_time_.top();
				pending_by_send_time_.pop();
				continue;
			}

			if (*next_time <= now) {
				auto msg = pending_by_send_time_.top();
				pending_by_send_time_.pop();
				return msg;
			} else {
				pending_by_send_cv_.wait_until(lock, *next_time, [&] { return stop_flag_.load(); });
			}
		} else {
			pending_by_send_cv_.wait(lock, [&] { return stop_flag_ || !pending_by_send_time_.empty(); });
		}
	}
}

void RadioTransmitMessageQueue::senderLoop() {
	while (true) {
		std::unique_lock<std::mutex> lock(mtx);

		pending_by_send_cv_.wait(lock, [&] { return stop_flag_ || !pending_by_send_time_.empty(); });
		if (stop_flag_) {
			return;
		}

		auto next_msg = pending_by_send_time_.top();
		const auto now = std::chrono::steady_clock::now();

		const auto &next_attempt_time = next_msg->state().next_attempt_time;
		D_ASSERT(next_attempt_time.has_value());

		if (*next_attempt_time <= now) {
			pending_by_send_time_.pop();
			active_sends_++;
			lock.unlock();

			update_message_state(next_msg, RadioTransmitMessageProcessingState::SENDING, RadioCurrentTimeMillis(), "");

			bool success = false;
			string result;
			if (stop_flag_) {
				result = "subscription stopped";
			} else {
				try {
					if (std::holds_alternative<std::unique_ptr<ix::WebSocket>>(subscription_.connection)) {
						auto &websocket = std::get<std::unique_ptr<ix::WebSocket>>(subscription_.connection);
						if (!websocket) {
							throw std::runtime_error("WebSocket is not initialized");
						}
						success = websocket->sendBinary(next_msg->message()).success;
						if (!success) {
							result = "WebSocket send failed";
						}
					} else if (std::holds_alternative<RedisSubscription>(subscription_.connection)) {
						if (!next_msg->channel().has_value()) {
							throw std::runtime_error("Redis transmit requires a channel");
						}
						auto &redis = std::get<RedisSubscription>(subscription_.connection);
						redis.redis->publish(next_msg->channel().value(), next_msg->message());
						success = true;
					} else {
						throw std::runtime_error("Subscription connection is not initialized");
					}
				} catch (const std::exception &error) {
					result = error.what();
				} catch (...) {
					result = "Unknown transport error";
				}
			}

			update_message_state(next_msg,
			                     success ? RadioTransmitMessageProcessingState::SENT
			                             : RadioTransmitMessageProcessingState::PENDING,
			                     RadioCurrentTimeMillis(), result);
			{
				std::lock_guard<std::mutex> send_lock(mtx);
				active_sends_--;
			}
			pending_by_send_cv_.notify_all();
		} else {
			pending_by_send_cv_.wait_until(lock, *next_attempt_time, [&] { return stop_flag_.load(); });
		}
	}
}

// std::shared_ptr<RadioTransmitMessage> RadioTransmitMessageQueue::pop(const MessageState state) {
// 	std::lock_guard<std::mutex> lock(mtx);

// 	switch (state) {
// 	case MessageState::PENDING: {
// 		auto top = pending_by_send_time_.top();
// 		pending_by_send_time_.pop();
// 		pending_map_.erase(top->id());
// 		return top;
// 	}
// 	case MessageState::PROCESSED: {
// 		if (processed_.empty()) {
// 			return nullptr;
// 		}
// 		auto item = processed_.front();
// 		processed_.pop_front();
// 		return item;
// 	}
// 	}
// }

std::vector<std::shared_ptr<RadioTransmitMessage>> RadioTransmitMessageQueue::snapshot() const {
	std::lock_guard<std::mutex> lock(mtx);
	std::vector<std::shared_ptr<RadioTransmitMessage>> result;
	result.reserve(messages_by_id_.size());
	for (const auto &item : messages_by_id_) {
		result.push_back(item.second);
	}
	return result;
}

// void RadioTransmitMessageQueue::resize(uint64_t new_capacity) {
// 	std::lock_guard<std::mutex> lock(mtx);
// 	processed_capacity_ = new_capacity;
// 	auto current_time = RadioCurrentTimeMillis();
// 	while (processed_.size() > processed_capacity_) {
// 		auto item = processed_.front();
// 		processed_.pop_front();
// 	}
// }

void RadioTransmitMessageQueue::remove_by_ids(const std::unordered_set<uint64_t> &ids_to_remove) {
	std::lock_guard<std::mutex> lock(mtx);
	for (uint64_t id : ids_to_remove) {
		messages_by_id_.erase(id);
	}

	rebuild_pending_retry_queue();
}

uint64_t RadioTransmitMessageQueue::size() const {
	std::lock_guard<std::mutex> lock(mtx);
	return messages_by_id_.size(); // Total size of all messages
}

// uint64_t RadioTransmitMessageQueue::capacity() const {
// 	std::lock_guard<std::mutex> lock(mtx);
// 	return processed_capacity_;
// }

bool RadioTransmitMessageQueue::empty() const {
	std::lock_guard<std::mutex> lock(mtx);
	return messages_by_id_.empty();
}

void RadioTransmitMessageQueue::clear() {
	std::lock_guard<std::mutex> lock(mtx);
	messages_by_id_.clear();
	pending_by_send_time_ = {};
	pending_by_send_cv_.notify_all();
}

void RadioTransmitMessageQueue::update_message_state(std::shared_ptr<RadioTransmitMessage> message,
                                                     const RadioTransmitMessageProcessingState new_state,
                                                     const uint64_t current_time, const std::string &transmit_result) {
	message->update_state(new_state, current_time, retry_initial_delay_ms_, retry_multiplier_, retry_max_delay_ms_,
	                      transmit_result);
	std::lock_guard<std::mutex> lock(mtx);
	auto after_update_state = message->state();
	if (after_update_state.state == RadioTransmitMessageProcessingState::SENT) {
		state_.successes++;
		state_.latest_success_time = std::max(current_time, state_.latest_success_time);
	} else if (after_update_state.state == RadioTransmitMessageProcessingState::RETRIES_EXHAUSTED ||
	           after_update_state.state == RadioTransmitMessageProcessingState::TIME_EXPIRED) {
		state_.failures++;
		state_.latest_failure_time = std::max(current_time, state_.latest_failure_time);
	}

	if (after_update_state.state == RadioTransmitMessageProcessingState::PENDING && !stop_flag_) {
		// The message may have been deleted while its transport call was in
		// progress. Only retry it if it is still the live map entry.
		auto entry = messages_by_id_.find(message->id());
		if (entry != messages_by_id_.end() && entry->second == message) {
			pending_by_send_time_.push(message);
		}
	}
	pending_by_send_cv_.notify_all();
}

RadioTransmitMessageQueueState RadioTransmitMessageQueue::state() const {
	std::lock_guard<std::mutex> lock(mtx);
	return state_;
}

bool RadioTransmitMessageQueue::flush_complete(const std::chrono::steady_clock::time_point &timeout) {
	std::unique_lock<std::mutex> lock(mtx);
	bool success = pending_by_send_cv_.wait_until(
	    lock, timeout, [&]() { return stop_flag_ || (pending_by_send_time_.empty() && active_sends_ == 0); });
	return success;
}

void RadioTransmitMessageQueue::start() {
	stop_flag_ = false;
	sender_thread_ = std::thread([this] { this->senderLoop(); });
}

void RadioTransmitMessageQueue::stop() {
	stop_flag_ = true;
	pending_by_send_cv_.notify_all();
	if (sender_thread_.joinable()) {
		sender_thread_.join();
	}
}

} // namespace duckdb
