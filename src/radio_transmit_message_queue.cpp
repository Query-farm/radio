#include "radio_transmit_message.hpp"
#include "radio_transmit_message_queue.hpp"

namespace duckdb {

void RadioTransmitMessageQueue::push(const std::vector<std::shared_ptr<RadioTransmitMessage>> &items) {
	std::lock_guard<std::mutex> lock(mtx);

	state_.odometer += items.size();
	for (auto &item : items) {
		pending_by_send_time_.push(item);
		messages_by_id_[item->id()] = item;
		state_.latest_queue_time = std::max(item->creation_time(), state_.latest_queue_time);
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

	pending_by_send_time_ = {};
	for (const auto &[_, msg] : messages_by_id_) {
		if (msg->state().state == RadioTransmitMessageProcessingState::PENDING) {
			pending_by_send_time_.push(msg);
		}
		break;
	}
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

	if (after_update_state.state == RadioTransmitMessageProcessingState::PENDING) {
		pending_by_send_time_.push(message);
	}
}

RadioTransmitMessageQueueState RadioTransmitMessageQueue::state() const {
	std::lock_guard<std::mutex> lock(mtx);
	return state_;
}

} // namespace duckdb