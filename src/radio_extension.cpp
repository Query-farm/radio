#define DUCKDB_EXTENSION_MAIN

#include "radio_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <deque>
#include <vector>
#include <iostream>
#include <optional>

#include "radio_received_message.hpp"
#include "radio_received_message_queue.hpp"
#include "radio_subscription.hpp"
#include "radio.hpp"

namespace duckdb {

Radio &GetRadio() {
	static Radio instance;
	return instance;
}

uint64_t current_time_millis() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
	        .count());
}

inline void RadioTuneInFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vector = args.data[0];

	UnaryExecutor::Execute<string_t, uint64_t>(url_vector, result, args.size(), [&](string_t url) {
		auto &radio = GetRadio();

		auto creation_time = current_time_millis();
		auto max_queued_messages = 10;
		return radio.AddSubscription(url.GetString(), max_queued_messages, creation_time)->get_id();
	});
}

inline void RadioTuneOutFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vector = args.data[0];

	UnaryExecutor::Execute<string_t, uint64_t>(url_vector, result, args.size(), [&](string_t url) {
		auto &radio = GetRadio();
		auto subscription = radio.GetSubscription(url.GetString());
		if (!subscription) {
			throw InvalidInputException("No subscription found for URL: " + url.GetString());
		}
		radio.RemoveSubscription(subscription->get_id());

		return subscription->get_id();
	});
}

inline void RadioIsTunedFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vector = args.data[0];

	UnaryExecutor::Execute<string_t, bool>(url_vector, result, args.size(), [&](string_t url) {
		auto &radio = GetRadio();
		auto subscription = radio.GetSubscription(url.GetString());
		if (!subscription) {
			return false;
		}
		return true;
	});
}

inline void RadioTransmitFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// auto &url_vector = args.data[0];
	// auto &message_vector = args.data[1];

	// UnaryExecutor::Execute<string_t, string_t>(url_vector, result, args.size(), [&](string_t name) {
	// 	return StringVector::AddString(result, "Quack " + name.GetString() + " 🐥");
	// });
}

inline void RadioTurnOffFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// UnaryExecutor::Execute<string_t, bool>(name_vector, result, args.size(), [&](string_t name) {
	// 	return StringVector::AddString(result, "Quack " + name.GetString() + " 🐥");
	// });
}

struct RadioSubscriptionsBindData : public TableFunctionData {
public:
	RadioSubscriptionsBindData(Radio &radio) {
		// Copy all subscriptions so we don't have to worry about changes
		// while scanning.
		subscriptions_ = radio.GetSubscriptions();
	}

	size_t rows_returned = 0;

	std::vector<std::shared_ptr<RadioSubscription>> subscriptions_;
};

void RadioSubscriptions(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioSubscriptionsBindData>();

	if (bind_data.rows_returned == bind_data.subscriptions_.size()) {
		output.SetCardinality(0);
		return;
	}

	// We can totally be lazy here and just return a single row per call
	// rather than filling the entire vector since this is all in memory data
	// and we can optimize this later.
	output.SetCardinality(1);

	auto &subscription = bind_data.subscriptions_[bind_data.rows_returned++];

	FlatVector::GetData<uint64_t>(output.data[0])[0] = subscription->get_id();

	FlatVector::GetData<string_t>(output.data[1])[0] =
	    StringVector::AddStringOrBlob(output.data[1], subscription->url());

	FlatVector::GetData<uint64_t>(output.data[2])[0] = subscription->creation_time();
	FlatVector::GetData<uint64_t>(output.data[3])[0] = subscription->activation_time();
	FlatVector::GetData<bool>(output.data[4])[0] = subscription->disabled() ? 1 : 0;
	FlatVector::GetData<uint64_t>(output.data[5])[0] =
	    subscription->get_latest_receive_time(RadioReceivedMessage::ERROR);
	FlatVector::GetData<uint64_t>(output.data[6])[0] =
	    subscription->get_latest_receive_time(RadioReceivedMessage::MESSAGE);

	FlatVector::GetData<uint64_t>(output.data[7])[0] = subscription->message_odometer(RadioReceivedMessage::ERROR);
	FlatVector::GetData<uint64_t>(output.data[8])[0] = subscription->message_odometer(RadioReceivedMessage::MESSAGE);

	output.SetCardinality(1);
}

static unique_ptr<FunctionData> RadioSubscriptionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {

	// id
	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("id");

	return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
	names.emplace_back("url");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("creation_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("activation_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
	names.emplace_back("disabled");

	// Ideally we'd have a struct since we have repeated fields per queue.

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("last_error_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("last_message_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("errors_processed");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("messages_processed");

	return make_uniq<RadioSubscriptionsBindData>(GetRadio());
}

struct RadioListenBindData : public TableFunctionData {
public:
	RadioListenBindData(Radio &radio, bool wait_for_messages, double wait_timeout = 0.0)
	    : wait_for_messages(wait_for_messages), wait_timeout(wait_timeout), returned_any_rows(false), radio(radio),
	      last_loop_after_waiting(false) {
		// Copy all subscriptions so we don't have to worry about changes
		// while scanning.
		subscriptions_ = radio.GetSubscriptions();
	}

	size_t current_subscription_index = 0;

	bool wait_for_messages;

	double wait_timeout = 0.0;

	bool returned_any_rows;

	Radio &radio;

	// Indicate we are looping through subscriptions one last time
	// after waiting for a message to arrive.
	bool last_loop_after_waiting = false;

	std::vector<std::shared_ptr<RadioSubscription>> subscriptions_;
};

static unique_ptr<FunctionData> RadioListenBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

	if (input.inputs.size() < 2) {
		throw BinderException("radio_listen requires at least 2 arguments");
	}

	auto wait_for_messages = input.inputs[0].GetValue<bool>();
	auto duration = input.inputs[1].GetValue<double>();

	// id
	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("id");

	return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
	names.emplace_back("url");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("messages_pending");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("errors_pending");

	return make_uniq<RadioListenBindData>(GetRadio(), wait_for_messages, duration);
}

void RadioListen(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioListenBindData>();

	auto process_subscription_index = [&]() -> bool {
		while (bind_data.current_subscription_index < bind_data.subscriptions_.size()) {
			auto &subscription = bind_data.subscriptions_[bind_data.current_subscription_index++];

			auto messages_pending = subscription->get_queue_size(RadioReceivedMessage::MESSAGE);
			auto errors_pending = subscription->get_queue_size(RadioReceivedMessage::ERROR);
			if (messages_pending == 0 && errors_pending == 0) {
				continue;
			}

			output.SetCardinality(1);
			FlatVector::GetData<uint64_t>(output.data[0])[0] = subscription->get_id();
			FlatVector::GetData<string_t>(output.data[1])[0] =
			    StringVector::AddStringOrBlob(output.data[1], subscription->url());
			FlatVector::GetData<uint64_t>(output.data[2])[0] = messages_pending;
			FlatVector::GetData<uint64_t>(output.data[3])[0] = errors_pending;
			bind_data.returned_any_rows = true;
			return true;
		}
		return false;
	};

	// If we're not waiting or we just finished waiting, try to process subscriptions immediately
	if (!bind_data.wait_for_messages || bind_data.last_loop_after_waiting) {
		if (!process_subscription_index()) {
			output.SetCardinality(0);
		}
		return;
	}

	// Loop through all subscriptions, if we any have messages they will
	// be indicated in bind_data.returned_any_rows
	if (process_subscription_index()) {
		return;
	}

	// When we iterated through subscriptions we didn't find messages.
	//
	// or we ran the last loop after waiting, and should just return
	// that we didn't find anything.
	if (!bind_data.returned_any_rows && !bind_data.last_loop_after_waiting) {
		// Wait for messages with timeout
		if (!bind_data.radio.WaitForMessages(
		        std::chrono::milliseconds(static_cast<int64_t>(bind_data.wait_timeout * 1000.0)))) {
			// No messages found during the timeout, so no rows returned
			// and no need to call again.
			output.SetCardinality(0);
			return;
		} else {
			// Loop through all subscriptions again.
			bind_data.current_subscription_index = 0;
			bind_data.last_loop_after_waiting = true;
			process_subscription_index();
			return;
		}
	}

	// After waiting, no messages found — return no rows
	output.SetCardinality(0);
}

static void LoadInternal(DatabaseInstance &instance) {

	// There are a few functions for the radio extension to process.

	// This should take an optional parameter for max number of messages.
	auto tune_in_function =
	    ScalarFunction("radio_tune_in", {LogicalType::VARCHAR}, LogicalType::UBIGINT, RadioTuneInFunction);
	ExtensionUtil::RegisterFunction(instance, tune_in_function);

	// tune_in_function.named_parameters["max_queued_messages"] = LogicalType::UINTEGER;

	auto tune_out_function =
	    ScalarFunction("radio_tune_out", {LogicalType::VARCHAR}, LogicalType::UBIGINT, RadioTuneOutFunction);
	ExtensionUtil::RegisterFunction(instance, tune_out_function);

	// auto transmit_function = ScalarFunction("transmit", {LogicalType::VARCHAR, LogicalType::BLOB},
	// LogicalType::BOOLEAN,
	//                                         RadioTransmitFunction);
	// ExtensionUtil::RegisterFunction(instance, transmit_function);

	// auto is_tuned_function =
	//     ScalarFunction("is_tuned", {LogicalType::VARCHAR}, LogicalType::BOOLEAN, RadioIsTunedFunction);
	// ExtensionUtil::RegisterFunction(instance, is_tuned_function);

	// auto turn_off_function =
	//     ScalarFunction("turn_off", {LogicalType::VARCHAR}, LogicalType::SQLNULL, RadioTurnOffFunction);
	// ExtensionUtil::RegisterFunction(instance, turn_off_function);

	// // This is a table function, just returns true if there are messages.
	// auto listen_function =
	//     TableFunction("listen", {LogicalType::BOOLEAN, LogicalType::DOUBLE}, RadioListenFunction,
	//     RadioListenBind);

	auto listen_function =
	    TableFunction("radio_listen", {LogicalType::BOOLEAN, LogicalType::DOUBLE}, RadioListen, RadioListenBind);
	ExtensionUtil::RegisterFunction(instance, listen_function);

	auto subscriptions_function = TableFunction("radio_subscriptions", {}, RadioSubscriptions, RadioSubscriptionsBind);
	ExtensionUtil::RegisterFunction(instance, subscriptions_function);

	// Table returning functions
	// incoming_messages

	// auto messages_function = TableFunction("radio_messages", {}, RadioMessages, RadioMessagesBind);
	// ExtensionUtil::RegisterFunction(instance, messages_function);

	// There is a table to get the actual messages in the catalog.
}

void RadioExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string RadioExtension::Name() {
	return "radio";
}

std::string RadioExtension::Version() const {
#ifdef EXT_VERSION_RADIO
	return EXT_VERSION_RADIO;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void radio_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::RadioExtension>();
}

DUCKDB_EXTENSION_API const char *radio_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
