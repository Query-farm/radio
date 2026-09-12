#pragma once
#include <sw/redis++/redis++.h>

struct RedisSubscription {
	std::unique_ptr<sw::redis::Redis> redis = nullptr;
	std::string channel_name;
};
