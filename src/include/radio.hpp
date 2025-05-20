
#pragma once
#include "radio_extension.hpp"

namespace duckdb {

class RadioSubscription;
class Radio {

public:
	explicit Radio() {
	}

	// These are all of the subscriptions that are currently activate.
	const std::vector<std::shared_ptr<RadioSubscription>> GetSubscriptions();

	// Add a new subscription to the list.
	std::shared_ptr<RadioSubscription> AddSubscription(const std::string &url, const uint32_t max_queued_messages,
	                                                   const uint64_t creation_time);

	std::shared_ptr<RadioSubscription> RemoveSubscription(const uint64_t id);
	void RemoveSubscription(std::shared_ptr<RadioSubscription> subscription);

	std::shared_ptr<RadioSubscription> GetSubscription(const uint64_t id);
	std::shared_ptr<RadioSubscription> GetSubscription(const std::string &url);

	bool IsActivelyTuned(const std::string &url);

	void turnoff();

	// Notify listeners that a message has arrived on some subscription.
	void NotifyHasMessages();

	// Wait for any messags to arrive on any subscription.
	bool WaitForMessages(std::chrono::milliseconds timeout);

private:
	// Need to protect the subscriptions map.
	mutable std::mutex mtx;

	std::map<uint64_t, std::shared_ptr<RadioSubscription>> subscriptions_;

	// Used for people to wait for messages to arrive.
	std::condition_variable cv;
	bool has_any_messages_ = false;

	uint64_t subscription_id_ = 0;
};
} // namespace duckdb