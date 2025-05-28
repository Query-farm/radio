#pragma once
#include "radio_extension.hpp"
#include "radio_received_message.hpp"

namespace duckdb {

class RadioReceivedMessageQueue {
public:
	RadioReceivedMessageQueue(size_t capacity) : capacity_(capacity) {
	}

	void push(const std::vector<std::shared_ptr<RadioReceivedMessage>> &items) {
		std::lock_guard<std::mutex> lock(mtx);

		odometer_ += items.size();
		for (auto &item : items) {
			if (queue_.size() >= capacity_) {
				auto &front = queue_.front();
				if (front->seen_count() == 0) {
					dropped_unseen_++;
				}
				queue_.pop_front(); // Drop oldest
			}
			latest_receive_time_ =
			    item->receive_time() > latest_receive_time_ ? item->receive_time() : latest_receive_time_;
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
			dropped_unseen_++;
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
				dropped_unseen_++;
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

	uint64_t dropped_unseen() const {
		std::lock_guard<std::mutex> lock(mtx);
		return dropped_unseen_;
	}

	bool empty() const {
		std::lock_guard<std::mutex> lock(mtx);
		return queue_.empty();
	}

	void clear() {
		std::lock_guard<std::mutex> lock(mtx);
		queue_.clear();
	}

	uint64_t latest_receive_time() const {
		std::lock_guard<std::mutex> lock(mtx);
		return latest_receive_time_;
	}

	uint64_t get_odometer() const {
		std::lock_guard<std::mutex> lock(mtx);
		return odometer_;
	}

private:
	uint64_t capacity_;

	uint64_t odometer_ = 0;

	uint64_t dropped_unseen_ = 0;

	std::deque<std::shared_ptr<RadioReceivedMessage>> queue_;

	uint64_t latest_receive_time_ = 0;

	mutable std::mutex mtx;
};
}