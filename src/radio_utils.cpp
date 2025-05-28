#include "radio_utils.hpp"

namespace duckdb {
uint64_t RadioCurrentTimeMillis() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
	        .count());
}
} // namespace duckdb
