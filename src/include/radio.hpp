
#pragma once
#include "duckdb.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "radio_subscription_parameters.hpp"

namespace duckdb {

class RadioSubscription;
class Radio : public ObjectCacheEntry {
public:
	Radio() = default;
	~Radio() override;

	static string ObjectType();
	string GetObjectType() override;
	optional_idx GetEstimatedCacheMemory() const override;

	// These are all of the subscriptions that are currently activate.
	const std::vector<std::shared_ptr<RadioSubscription>> GetSubscriptions();

	// Add a new subscription to the list.
	std::shared_ptr<RadioSubscription>
	AddSubscription(const std::string &url, const RadioSubscriptionParameters &params, const uint64_t creation_time);

	std::shared_ptr<RadioSubscription> RemoveSubscription(const uint64_t id);
	void RemoveSubscription(std::shared_ptr<RadioSubscription> subscription);

	std::shared_ptr<RadioSubscription> GetSubscription(const uint64_t id);
	std::shared_ptr<RadioSubscription> GetSubscription(const std::string &url);

	bool IsActivelyTuned(const std::string &url);

	void turnoff();
	void Shutdown() noexcept;

	// Notify listeners that a message has arrived on some subscription.
	void NotifyHasMessages();

	// A counter that increases every time a message arrives on any
	// subscription. Read it before scanning subscriptions, then pass it to
	// WaitForMessages() so a message that arrives during the scan is not lost.
	uint64_t MessageGeneration() const;

	// Wait until a message arrives after `seen_generation` was read, the
	// timeout passes, or the radio shuts down. Returns true if the message
	// generation moved past `seen_generation`.
	bool WaitForMessages(uint64_t seen_generation, std::chrono::milliseconds timeout);

private:
	// Need to protect the subscriptions map.
	mutable std::mutex mtx;

	std::map<uint64_t, std::shared_ptr<RadioSubscription>> subscriptions_;

	// Used for people to wait for messages to arrive.
	std::condition_variable cv;
	uint64_t message_generation_ = 0;

	uint64_t subscription_id_ = 0;
	bool shutting_down_ = false;
};
} // namespace duckdb
