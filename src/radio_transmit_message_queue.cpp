#include "radio_transmit_message.hpp"
#include "radio_transmit_message_queue.hpp"

namespace duckdb {
void RadioTransmitMessageQueue::push(const std::vector<std::shared_ptr<RadioTransmitMessage>> &items,
                                     const uint64_t current_time) {
	std::lock_guard<std::mutex> lock(mtx);

	state_.odometer += items.size();
	for (auto &item : items) {
		if (queue_.size() >= capacity_) {
			auto &front = queue_.front();
			if (!front->was_processed(current_time)) {
				state_.dropped_unprocessed++;
			}
			queue_.pop_front(); // Drop oldest
		}
		state_.latest_queue_time =
		    item->creation_time() > state_.latest_queue_time ? item->creation_time() : state_.latest_queue_time;
		queue_.push_back(item);
	}
}

int32_t RadioTransmitMessageQueue::retry_delay_time_ms(const RadioTransmitMessage &message) const {
	auto delay = static_cast<int32_t>(retry_initial_delay_ms_ * std::pow(retry_multiplier_, message.state().try_count));
	delay = std::min(delay, retry_max_delay_ms_);
	return delay;
}

std::shared_ptr<RadioTransmitMessage> RadioTransmitMessageQueue::pop(uint64_t current_time) {
	std::lock_guard<std::mutex> lock(mtx);
	if (queue_.empty()) {
		return nullptr;
	}
	auto item = queue_.front();
	if (item->was_processed(current_time) == 0) {
		state_.dropped_unprocessed++;
	}
	queue_.pop_front();
	return item;
}

std::vector<std::shared_ptr<RadioTransmitMessage>> RadioTransmitMessageQueue::snapshot() const {
	std::lock_guard<std::mutex> lock(mtx);
	return std::vector<std::shared_ptr<RadioTransmitMessage>>(queue_.begin(), queue_.end());
}

void RadioTransmitMessageQueue::resize(uint64_t new_capacity) {
	std::lock_guard<std::mutex> lock(mtx);
	capacity_ = new_capacity;
	auto current_time = RadioCurrentTimeMillis();
	while (queue_.size() > capacity_) {
		auto item = queue_.front();
		if (item->was_processed(current_time) == 0) {
			state_.dropped_unprocessed++;
		}
		queue_.pop_front();
	}
}

void RadioTransmitMessageQueue::remove_by_ids(const std::unordered_set<uint64_t> &ids_to_remove) {
	std::lock_guard<std::mutex> lock(mtx);
	queue_.erase(std::remove_if(queue_.begin(), queue_.end(),
	                            [&](const std::shared_ptr<RadioTransmitMessage> &item) {
		                            return ids_to_remove.count(item->id()) > 0;
	                            }),
	             queue_.end());
}

uint64_t RadioTransmitMessageQueue::size() const {
	std::lock_guard<std::mutex> lock(mtx);
	return queue_.size();
}

uint64_t RadioTransmitMessageQueue::capacity() const {
	std::lock_guard<std::mutex> lock(mtx);
	return capacity_;
}

uint64_t RadioTransmitMessageQueue::dropped_unprocessed() const {
	std::lock_guard<std::mutex> lock(mtx);
	return state_.dropped_unprocessed;
}

bool RadioTransmitMessageQueue::empty() const {
	std::lock_guard<std::mutex> lock(mtx);
	return queue_.empty();
}

void RadioTransmitMessageQueue::clear() {
	std::lock_guard<std::mutex> lock(mtx);
	queue_.clear();
}

void RadioTransmitMessageQueue::record_transmit_attempt(bool success, uint64_t last_attempt_time) {
	std::lock_guard<std::mutex> lock(mtx);
	if (success) {
		state_.transmit_successes++;
		state_.latest_success_time =
		    state_.latest_success_time < last_attempt_time ? last_attempt_time : state_.latest_success_time;
	} else {
		state_.transmit_failures++;
		state_.latest_failure_time =
		    state_.latest_failure_time < last_attempt_time ? last_attempt_time : state_.latest_failure_time;
	}
}

RadioTransmitMessageQueueState RadioTransmitMessageQueue::state() const {
	std::lock_guard<std::mutex> lock(mtx);
	return state_;
}

} // namespace duckdb