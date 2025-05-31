#pragma once

namespace duckdb {

struct RadioSubscriptionParameters {
	int32_t receive_message_capacity = 1000;

	int32_t transmit_retry_initial_delay_ms = 100;
	double transmit_retry_multiplier = 1.5;
	int32_t transmit_retry_max_delay_ms = 10000;
};

}