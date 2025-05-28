#pragma once
#include "radio_extension.hpp"
#include "radio_received_message.hpp"

namespace duckdb {

struct RadioReceivedMessageQueueState {
	uint64_t odometer = 0;

	uint64_t dropped_unseen = 0;

	uint64_t latest_receive_time = 0;
};

class RadioReceivedMessageQueue {
public:
	RadioReceivedMessageQueue(size_t capacity) : capacity_(capacity) {
	}

	void push(const std::vector<std::shared_ptr<RadioReceivedMessage>> &items) {
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

	std::shared_ptr<RadioReceivedMessage> pop() {
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

	std::vector<std::shared_ptr<RadioReceivedMessage>> snapshot() const {
		std::lock_guard<std::mutex> lock(mtx);
		for (const auto &item : queue_) {
			item->increment_seen_count();
		}
		return std::vector<std::shared_ptr<RadioReceivedMessage>>(queue_.begin(), queue_.end());
	}

	void resize(uint64_t new_capacity) {
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

	void remove_by_ids(const std::unordered_set<uint64_t> &ids_to_remove) {
		std::lock_guard<std::mutex> lock(mtx);
		queue_.erase(std::remove_if(queue_.begin(), queue_.end(),
		                            [&](const std::shared_ptr<RadioReceivedMessage> &item) {
			                            return ids_to_remove.count(item->id()) > 0;
		                            }),
		             queue_.end());
	}

	uint64_t size() const {
		std::lock_guard<std::mutex> lock(mtx);
		return queue_.size();
	}

	uint64_t capacity() const {
		std::lock_guard<std::mutex> lock(mtx);
		return capacity_;
	}

	bool empty() const {
		std::lock_guard<std::mutex> lock(mtx);
		return queue_.empty();
	}

	void clear() {
		std::lock_guard<std::mutex> lock(mtx);
		queue_.clear();
	}

	RadioReceivedMessageQueueState state() const {
		std::lock_guard<std::mutex> lock(mtx);
		return state_;
	}

private:
	uint64_t capacity_;

	RadioReceivedMessageQueueState state_;

	std::deque<std::shared_ptr<RadioReceivedMessage>> queue_;

	mutable std::mutex mtx;
};
}