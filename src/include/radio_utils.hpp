#pragma once
#include "duckdb.hpp"

namespace duckdb {

class ClientContext;

uint64_t RadioCurrentTimeMillis();

// Radio opens network connections (WebSocket and Redis). Throws a
// PermissionException when enable_external_access is false.
void RadioCheckExternalAccess(ClientContext &context, const char *function_name);

// Sleep until `deadline` in short slices, throwing InterruptException if the
// query is interrupted.
void RadioInterruptibleSleepUntil(ClientContext &context, std::chrono::steady_clock::time_point deadline);

// Throws InterruptException if the query has been interrupted.
void RadioCheckInterrupted(ClientContext &context);

// How long blocking waits sleep before re-checking for interruption.
static constexpr std::chrono::milliseconds RADIO_INTERRUPT_POLL_INTERVAL {100};

// A named parameter that is still passed by position only, so calls work as before.
inline FunctionParameter Positional(const char *name, LogicalType type) {
	return FunctionParameter(name, std::move(type), {}, FunctionParameterKind::POSITIONAL_ONLY);
}

// The signature of a table function, with named parameters.
inline FunctionSignature TableSignature(vector<FunctionParameter> parameters) {
	return FunctionSignature(std::move(parameters), LogicalType::INVALID);
}

#define STORE_NULLABLE_TIMESTAMP(destination, row, source)                                                             \
	if ((source) == 0) {                                                                                               \
		FlatVector::SetNull(destination, row, true);                                                                   \
	} else {                                                                                                           \
		FlatVector::GetDataMutable<int64_t>(destination)[row] = static_cast<int64_t>(source);                          \
	}

} // namespace duckdb

namespace duckdb {

// Returns the bind-time argument at `index`, throwing a user-facing error if
// it is NULL.
inline const Value &RadioRequireArgument(const TableFunctionBindInput &input, idx_t index, const char *function_name,
                                         const char *parameter_name) {
	auto &value = input.inputs[index];
	if (value.IsNull()) {
		throw InvalidInputException("%s requires a non-NULL %s", function_name, parameter_name);
	}
	return value;
}

// A table function state for functions that produce a single row each time
// they execute, e.g. radio_subscribe(). Kept out of the bind data so that a
// prepared statement can be executed more than once.
struct RadioSingleRowState : public GlobalTableFunctionState {
	bool done = false;
};

inline unique_ptr<GlobalTableFunctionState> RadioSingleRowInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RadioSingleRowState>();
}

} // namespace duckdb
