#include "radio_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"

#include <thread>

namespace duckdb {
uint64_t RadioCurrentTimeMillis() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
	        .count());
}

void RadioCheckExternalAccess(ClientContext &context, const char *function_name) {
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("%s is disabled because enable_external_access is false", function_name);
	}
}

void RadioCheckInterrupted(ClientContext &context) {
	if (context.IsInterrupted()) {
		throw InterruptException();
	}
}

void RadioInterruptibleSleepUntil(ClientContext &context, std::chrono::steady_clock::time_point deadline) {
	while (true) {
		RadioCheckInterrupted(context);
		auto now = std::chrono::steady_clock::now();
		if (now >= deadline) {
			return;
		}
		auto slice = std::min<std::chrono::steady_clock::duration>(deadline - now, RADIO_INTERRUPT_POLL_INTERVAL);
		std::this_thread::sleep_for(slice);
	}
}
} // namespace duckdb
