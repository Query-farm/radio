#include "radio_extension.hpp"
#include "radio_subscription.hpp"
#include "radio.hpp"

namespace duckdb {

const std::vector<std::shared_ptr<RadioSubscription>> Radio::GetSubscriptions() {
	std::lock_guard<std::mutex> lock(mtx);

	auto result = std::vector<std::shared_ptr<RadioSubscription>>();
	for (const auto &sub : subscriptions_) {
		result.push_back(sub.second);
	}
	return result;
}

// Add a new subscription to the list.
std::shared_ptr<RadioSubscription> Radio::AddSubscription(const std::string &url, const uint32_t max_queued_messages,
                                                          const uint64_t creation_time) {
	// Check to see if we're already subscribed to this url.
	std::lock_guard<std::mutex> lock(mtx);

	auto it = std::find_if(subscriptions_.begin(), subscriptions_.end(),
	                       [&url](const auto &sub) { return sub.second->url() == url; });
	if (it != subscriptions_.end()) {
		// We are already subscribed to this URL, so we can just return.
		return it->second;
	}
	auto new_id = subscription_id_++;
	subscriptions_[new_id] =
	    std::make_shared<RadioSubscription>(new_id, url, max_queued_messages, creation_time, *this);
	std::make_shared<RadioSubscription>(new_id, url, max_queued_messages, creation_time, *this);
	return subscriptions_[new_id];
}

void Radio::RemoveSubscription(std::shared_ptr<RadioSubscription> subscription) {
	std::lock_guard<std::mutex> lock(mtx);

	auto it = subscriptions_.find(subscription->get_id());
	if (it != subscriptions_.end()) {
		auto removed = std::move(it->second);
		subscriptions_.erase(it);
	}
}

std::shared_ptr<RadioSubscription> Radio::RemoveSubscription(const uint64_t id) {
	std::lock_guard<std::mutex> lock(mtx);

	auto it = subscriptions_.find(id);
	if (it != subscriptions_.end()) {
		auto removed = std::move(it->second);
		subscriptions_.erase(it);
		return removed;
	}
	return nullptr;
}

void Radio::turnoff() {
	std::lock_guard<std::mutex> lock(mtx);

	for (auto &sub : subscriptions_) {
		sub.second->set_disabled(true);
	}
}

std::shared_ptr<RadioSubscription> Radio::GetSubscription(const uint64_t id) {
	std::lock_guard<std::mutex> lock(mtx);

	auto it = subscriptions_.find(id);
	return it != subscriptions_.end() ? it->second : nullptr;
}

std::shared_ptr<RadioSubscription> Radio::GetSubscription(const std::string &url) {
	std::lock_guard<std::mutex> lock(mtx);

	auto it = std::find_if(subscriptions_.begin(), subscriptions_.end(),
	                       [&url](const auto &sub) { return sub.second->url() == url; });
	if (it != subscriptions_.end()) {
		return it->second;
	}
	return nullptr;
}

bool Radio::IsActivelyTuned(const std::string &url) {
	std::lock_guard<std::mutex> lock(mtx);

	auto it = std::find_if(subscriptions_.begin(), subscriptions_.end(),
	                       [&url](const auto &sub) { return sub.second->url() == url; });
	if (it != subscriptions_.end()) {
		return !it->second->disabled();
	}
	return false;
}

void Radio::NotifyHasMessages() {
	{
		std::lock_guard<std::mutex> lock(mtx);
		has_any_messages_ = true;
	}
	cv.notify_one();
}

bool Radio::WaitForMessages(std::chrono::milliseconds timeout) {
	std::unique_lock<std::mutex> lock(mtx);
	bool success = cv.wait_for(lock, timeout, [this] { return has_any_messages_; });

	return success;
}

} // namespace duckdb