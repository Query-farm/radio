#pragma once
#include "duckdb.hpp"

namespace duckdb {

uint64_t RadioCurrentTimeMillis();

// A named parameter that is still passed by position only, so calls work as before.
inline FunctionParameter Positional(const char *name, LogicalType type) {
	return FunctionParameter(name, std::move(type), {}, FunctionParameterKind::POSITIONAL_ONLY);
}

// The signature of a table function, with named parameters.
inline FunctionSignature TableSignature(vector<FunctionParameter> parameters) {
	return FunctionSignature(std::move(parameters), LogicalType::INVALID);
}

#define STORE_NULLABLE_TIMESTAMP(destination, source)                                                                  \
	if (source == 0) {                                                                                                 \
		FlatVector::SetNull(destination, 0, true);                                                                     \
	} else {                                                                                                           \
		FlatVector::GetDataMutable<int64_t>(destination)[0] = static_cast<int64_t>(source);                            \
	}

} // namespace duckdb
