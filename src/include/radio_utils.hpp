#pragma once
#include "duckdb.hpp"

namespace duckdb {

uint64_t RadioCurrentTimeMillis();

#define STORE_NULLABLE_TIMESTAMP(destination, source)                                                                  \
	if (source == 0) {                                                                                                 \
		FlatVector::SetNull(destination, 0, true);                                                                     \
	} else {                                                                                                           \
		FlatVector::GetData<uint64_t>(destination)[0] = source;                                                        \
	}

} // namespace duckdb
