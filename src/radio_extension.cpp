#define DUCKDB_EXTENSION_MAIN

#include "radio_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "radio_received_message.hpp"
#include "radio_received_message_queue.hpp"
#include "radio_subscription.hpp"
#include "radio_utils.hpp"
#include "radio.hpp"

#include "query_farm_telemetry.hpp"

namespace duckdb {

Radio &GetRadio() {
	static Radio instance;
	return instance;
}

static LogicalType CreateEnumType(const string &name, const vector<string> &members) {
	auto varchar_vector = Vector(LogicalType::VARCHAR, members.size());
	auto varchar_data = FlatVector::GetData<string_t>(varchar_vector);
	for (idx_t i = 0; i < members.size(); i++) {
		auto str = string_t(members[i]);
		varchar_data[i] = str.IsInlined() ? str : StringVector::AddString(varchar_vector, str);
	}
	auto enum_type = LogicalType::ENUM(name, varchar_vector, members.size());
	enum_type.SetAlias(name);
	return enum_type;
}

struct RadioSubscribeBindData : public TableFunctionData {
	explicit RadioSubscribeBindData(Radio &radio, const string &url, RadioSubscriptionParameters &params,
	                                uint64_t creation_time)
	    : radio_(radio), url_(url), creation_time_(creation_time), params_(params) {
	}

	Radio &radio_;
	string url_;
	uint64_t creation_time_;
	RadioSubscriptionParameters params_;

	bool did_subscribe = false;
};

static unique_ptr<FunctionData> RadioSubscribeBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {

	auto url = input.inputs[0].GetValue<string>();

	// FIXME: deal with the named parameters and the default values.
	RadioSubscriptionParameters params;

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "receive_message_capacity") {
			params.receive_message_capacity = kv.second.GetValue<int32_t>();
			if (params.receive_message_capacity <= 0) {
				throw BinderException("radio_subscribe requires receive_message_capacity to be greater than 0");
			}
			// } else if (loption == "receive_error_capacity") {
			// 	params.receive_error_capacity = kv.second.GetValue<int32_t>();
			// 	if (params.receive_error_capacity <= 0) {
			// 		throw BinderException("radio_subscribe requires receive_error_capacity to be greater than 0");
			// 	}
			// } else if (loption == "transmit_message_capacity") {
			// 	params.transmit_message_capacity = kv.second.GetValue<int32_t>();
			// 	if (params.transmit_message_capacity <= 0) {
			// 		throw BinderException("radio_subscribe requires transmit_message_capacity to be greater than 0");
			// 	}
		} else if (loption == "transmit_retry_initial_delay_ms") {
			params.transmit_retry_initial_delay_ms = kv.second.GetValue<int32_t>();
			if (params.transmit_retry_initial_delay_ms <= 0) {
				throw BinderException("radio_subscribe requires transmit_retry_initial_delay_ms to be non-negative");
			}
		} else if (loption == "transmit_retry_multiplier") {
			params.transmit_retry_multiplier = kv.second.GetValue<double>();
			if (params.transmit_retry_multiplier <= 1.0) {
				throw BinderException("radio_subscribe requires transmit_retry_multiplier to be greater than 1.0");
			}
		} else if (loption == "transmit_retry_max_delay_ms") {
			params.transmit_retry_max_delay_ms = kv.second.GetValue<int32_t>();
			if (params.transmit_retry_max_delay_ms <= 0) {
				throw BinderException("radio_subscribe requires transmit_retry_max_delay_ms to be greater than 0");
			}
		} else {
			throw BinderException("Unknown named parameter for radio_subscribe: " + kv.first);
		}
	}

	auto creation_time = RadioCurrentTimeMillis();

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("subscription_id");

	return make_uniq<RadioSubscribeBindData>(GetRadio(), url, params, creation_time);
}

void RadioSubscribe(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioSubscribeBindData>();

	if (bind_data.did_subscribe) {
		// No more messages to return.
		output.SetCardinality(0);
		return;
	}

	// Return a row at a time for now.
	bind_data.did_subscribe = true;
	output.SetCardinality(1);

	auto subscription = bind_data.radio_.AddSubscription(bind_data.url_, bind_data.params_, bind_data.creation_time_);

	FlatVector::GetData<uint64_t>(output.data[0])[0] = subscription->id();
}

struct RadioUnsubscribeBindData : public TableFunctionData {
	explicit RadioUnsubscribeBindData(Radio &radio, const string &url) : radio_(radio), url_(url) {
	}

	Radio &radio_;
	string url_;

	bool did_unsubscribe = false;
};

static unique_ptr<FunctionData> RadioUnsubscribeBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {

	auto url = input.inputs[0].GetValue<string>();

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("subscription_id");

	return make_uniq<RadioUnsubscribeBindData>(GetRadio(), url);
}

void RadioUnsubscribe(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioUnsubscribeBindData>();

	if (bind_data.did_unsubscribe) {
		// No more messages to return.
		output.SetCardinality(0);
		return;
	}

	// Return a row at a time for now.
	bind_data.did_unsubscribe = true;
	output.SetCardinality(1);

	auto subscription = bind_data.radio_.GetSubscription(bind_data.url_);
	if (!subscription) {
		throw InvalidInputException("No subscription found for URL: " + bind_data.url_);
	}
	bind_data.radio_.RemoveSubscription(subscription->id());

	FlatVector::GetData<uint64_t>(output.data[0])[0] = subscription->id();
}

inline void RadioUnsubscribeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vector = args.data[0];

	UnaryExecutor::Execute<string_t, uint64_t>(url_vector, result, args.size(), [&](string_t url) {
		auto &radio = GetRadio();
		auto subscription = radio.GetSubscription(url.GetString());
		if (!subscription) {
			throw InvalidInputException("No subscription found for URL: " + url.GetString());
		}
		radio.RemoveSubscription(subscription->id());

		return subscription->id();
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

	auto state = subscription->state();

	FlatVector::GetData<uint64_t>(output.data[0])[0] = subscription->id();

	FlatVector::GetData<string_t>(output.data[1])[0] =
	    StringVector::AddStringOrBlob(output.data[1], subscription->url());

	FlatVector::GetData<uint64_t>(output.data[2])[0] = subscription->creation_time();

	STORE_NULLABLE_TIMESTAMP(output.data[3], subscription->activation_time());
	FlatVector::GetData<bool>(output.data[4])[0] = subscription->disabled() ? 1 : 0;

	STORE_NULLABLE_TIMESTAMP(output.data[5], state.received.latest_receive_time);

	FlatVector::GetData<uint64_t>(output.data[6])[0] = state.received.odometer;

	FlatVector::GetData<uint64_t>(output.data[7])[0] = state.transmit.odometer;

	STORE_NULLABLE_TIMESTAMP(output.data[8], state.transmit.latest_queue_time);
	STORE_NULLABLE_TIMESTAMP(output.data[9], state.transmit.latest_success_time);
	STORE_NULLABLE_TIMESTAMP(output.data[10], state.transmit.latest_failure_time);

	FlatVector::GetData<uint64_t>(output.data[11])[0] = state.transmit.successes;
	FlatVector::GetData<uint64_t>(output.data[12])[0] = state.transmit.failures;

	output.SetCardinality(1);
}

static unique_ptr<FunctionData> RadioSubscriptionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {

	// id
	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("subscription_id");

	return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
	names.emplace_back("url");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("creation_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("activation_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
	names.emplace_back("disabled");

	// Ideally we'd have a struct since we have repeated fields per queue.
	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("received_last_message_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("received_messages_processed");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("transmit_messages_processed");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("transmit_last_queue_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("transmit_last_success_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("transmit_last_failure_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("transmit_successes");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("transmit_failures");

	return make_uniq<RadioSubscriptionsBindData>(GetRadio());
}

struct RadioSleepBindData : public TableFunctionData {
public:
	RadioSleepBindData(double duration_ms) : duration(duration_ms) {
	}

	bool did_sleep = false;
	double duration = 0.0;
};

static unique_ptr<FunctionData> RadioSleepBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("radio_sleep requires one argument");
	}

	auto duration = input.inputs[0].GetValue<interval_t>();

	return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
	names.emplace_back("finished");

	return make_uniq<RadioSleepBindData>(Interval::GetMilli(duration));
}

void RadioSleep(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioSleepBindData>();

	if (bind_data.did_sleep) {
		// No more messages to return.
		output.SetCardinality(0);
		return;
	}

	// Return a row at a time for now.
	bind_data.did_sleep = true;
	output.SetCardinality(1);
	FlatVector::GetData<bool>(output.data[0])[0] = true;

	// Sleep for the specified duration
	std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int64_t>(bind_data.duration)));
}

struct RadioFlushBindData : public TableFunctionData {
public:
	RadioFlushBindData(Radio &radio, int64_t wait_timeout_ms)
	    : wait_timeout_ms(wait_timeout_ms), returned_any_rows(false), radio(radio) {
		// Copy all subscriptions so we don't have to worry about changes
		// while scanning.
		subscriptions_ = radio.GetSubscriptions();
	}

	size_t current_subscription_index = 0;

	const int64_t wait_timeout_ms = 0;

	bool returned_any_rows = false;

	Radio &radio;

	std::vector<std::shared_ptr<RadioSubscription>> subscriptions_;
};

static unique_ptr<FunctionData> RadioFlushBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("radio_flush requires at least 1 argument");
	}

	auto duration = input.inputs[0].GetValue<interval_t>();

	auto duration_milliseconds = Interval::GetMilli(duration);
	if (duration_milliseconds < 0) {
		throw BinderException("radio_flush requires the argument to be a non-negative interval");
	}

	return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
	names.emplace_back("all_messages_flushed");

	return make_uniq<RadioFlushBindData>(GetRadio(), duration_milliseconds);
}

void RadioFlush(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioFlushBindData>();

	const auto now = std::chrono::steady_clock::now();
	const auto finish_time = now + std::chrono::milliseconds(bind_data.wait_timeout_ms);

	if (bind_data.returned_any_rows) {
		// We already returned rows, so we don't need to do anything.
		output.SetCardinality(0);
		return;
	}

	bind_data.returned_any_rows = true;
	output.SetCardinality(1);

	for (auto &subscription : bind_data.subscriptions_) {
		if (!subscription->flush_complete(finish_time)) {
			FlatVector::GetData<bool>(output.data[0])[0] = 0;
			return;
		}
	}

	// Everything was flushed successfully, return a single row with true.
	FlatVector::GetData<bool>(output.data[0])[0] = 1;
}

struct RadioListenBindData : public TableFunctionData {
public:
	RadioListenBindData(Radio &radio, bool wait_for_messages, int64_t wait_timeout_ms)
	    : wait_for_messages(wait_for_messages), wait_timeout_ms(wait_timeout_ms), returned_any_rows(false),
	      radio(radio), last_loop_after_waiting(false) {
		// Copy all subscriptions so we don't have to worry about changes
		// while scanning.
		subscriptions_ = radio.GetSubscriptions();
	}

	size_t current_subscription_index = 0;

	bool wait_for_messages;

	const int64_t wait_timeout_ms = 0;

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
	auto duration = input.inputs[1].GetValue<interval_t>();

	auto duration_milliseconds = Interval::GetMilli(duration);
	if (duration_milliseconds < 0) {
		throw BinderException("radio_listen requires the second argument to be a non-negative interval");
	}

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("subscription_id");

	return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
	names.emplace_back("subscription_url");

	return make_uniq<RadioListenBindData>(GetRadio(), wait_for_messages, duration_milliseconds);
}

void RadioListen(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioListenBindData>();

	auto check_subscriptions_for_messages = [&]() -> bool {
		while (bind_data.current_subscription_index < bind_data.subscriptions_.size()) {
			auto &subscription = bind_data.subscriptions_[bind_data.current_subscription_index++];

			if (!subscription->has_unseen()) {
				continue;
			}

			output.SetCardinality(1);
			FlatVector::GetData<uint64_t>(output.data[0])[0] = subscription->id();
			FlatVector::GetData<string_t>(output.data[1])[0] =
			    StringVector::AddStringOrBlob(output.data[1], subscription->url());
			bind_data.returned_any_rows = true;
			return true;
		}
		return false;
	};

	// If we're not waiting or we just finished waiting, try to process subscriptions immediately
	if (!bind_data.wait_for_messages || bind_data.last_loop_after_waiting) {
		if (!check_subscriptions_for_messages()) {
			output.SetCardinality(0);
		}
		return;
	}

	// Loop through all subscriptions, if we any have messages they will
	// be indicated in bind_data.returned_any_rows
	if (check_subscriptions_for_messages()) {
		return;
	}

	// When we iterated through subscriptions we didn't find messages.
	//
	// or we ran the last loop after waiting, and should just return
	// that we didn't find anything.
	if (!bind_data.returned_any_rows && !bind_data.last_loop_after_waiting) {
		// Wait for messages with timeout
		if (!bind_data.radio.WaitForMessages(std::chrono::milliseconds(bind_data.wait_timeout_ms))) {
			// No messages found during the timeout, so no rows returned
			// and no need to call again.
			output.SetCardinality(0);
			return;
		} else {
			// Loop through all subscriptions again.
			bind_data.current_subscription_index = 0;
			bind_data.last_loop_after_waiting = true;
			check_subscriptions_for_messages();
			return;
		}
	}

	// After waiting, no messages found — return no rows
	output.SetCardinality(0);
}

struct RadioReceivedMessagesBindData : public TableFunctionData {
	explicit RadioReceivedMessagesBindData(Radio &radio) {
		for (auto &subscription : radio.GetSubscriptions()) {
			// Add all messages for each subscription.
			for (auto &message : subscription->receive_snapshot()) {
				messages_.emplace_back(message);
			}
		}
	}

	size_t current_row = 0;
	vector<std::shared_ptr<RadioReceivedMessage>> messages_;
};

static unique_ptr<FunctionData> RadioReceivedMessagesBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("subscription_id");

	return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
	names.emplace_back("subscription_url");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("message_id");

	return_types.emplace_back(
	    CreateEnumType("received_message_type", {"message", "error", "connection", "disconnection"}));
	names.emplace_back("message_type");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("receive_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("seen_count");

	return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
	names.emplace_back("channel");

	return_types.emplace_back(LogicalType(LogicalTypeId::BLOB));
	names.emplace_back("message");

	return make_uniq<RadioReceivedMessagesBindData>(GetRadio());
}

void RadioReceivedMessages(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioReceivedMessagesBindData>();

	if (bind_data.current_row >= bind_data.messages_.size()) {
		// No more messages to return.
		output.SetCardinality(0);
		return;
	}

	// Return a row at a time for now.
	auto &message = bind_data.messages_[bind_data.current_row++];
	output.SetCardinality(1);

	auto &subscription = message->subscription();

	// From the subscription
	FlatVector::GetData<uint64_t>(output.data[0])[0] = subscription.id();
	FlatVector::GetData<string_t>(output.data[1])[0] =
	    StringVector::AddStringOrBlob(output.data[1], subscription.url());

	// From the message
	FlatVector::GetData<uint64_t>(output.data[2])[0] = message->id();
	FlatVector::GetData<uint8_t>(output.data[3])[0] = RadioReceivedMessage::message_type_to_enum_index(message->type());

	FlatVector::GetData<int64_t>(output.data[4])[0] = message->receive_time();
	FlatVector::GetData<uint64_t>(output.data[5])[0] = message->seen_count();

	if (message->channel().has_value()) {
		FlatVector::GetData<string_t>(output.data[6])[0] =
		    StringVector::AddStringOrBlob(output.data[6], *message->channel());
	} else {
		FlatVector::SetNull(output.data[6], 0, true);
	}

	FlatVector::GetData<string_t>(output.data[7])[0] =
	    StringVector::AddStringOrBlob(output.data[7], message->message());
}

inline void RadioVersionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result.SetValue(0, Value("20250601.02"));
}

static void LoadInternal(ExtensionLoader &loader) {
	// There are a few functions for the radio extension to process.

	// This should take an optional parameter for max number of messages.

	auto subscribe_function =
	    TableFunction("radio_subscribe", {LogicalType::VARCHAR}, RadioSubscribe, RadioSubscribeBind);
	subscribe_function.named_parameters["receive_message_capacity"] = LogicalType::INTEGER;
	// subscribe_function.named_parameters["receive_error_capacity"] = LogicalType::INTEGER;
	//  subscribe_function.named_parameters["transmit_message_capacity"] = LogicalType::INTEGER;

	subscribe_function.named_parameters["transmit_retry_initial_delay_ms"] = LogicalType::INTEGER;
	subscribe_function.named_parameters["transmit_retry_multiplier"] = LogicalType::DOUBLE;
	subscribe_function.named_parameters["transmit_retry_max_delay_ms"] = LogicalType::INTEGER;

	loader.RegisterFunction(subscribe_function);

	auto unsubscribe_function =
	    TableFunction("radio_unsubscribe", {LogicalType::VARCHAR}, RadioUnsubscribe, RadioUnsubscribeBind);
	loader.RegisterFunction(unsubscribe_function);

	auto version_function = ScalarFunction("radio_version", {}, LogicalType::VARCHAR, RadioVersionFunction);
	loader.RegisterFunction(version_function);

	auto listen_function =
	    TableFunction("radio_listen", {LogicalType::BOOLEAN, LogicalType::INTERVAL}, RadioListen, RadioListenBind);
	loader.RegisterFunction(listen_function);

	auto flush_function = TableFunction("radio_flush", {LogicalType::INTERVAL}, RadioFlush, RadioFlushBind);
	loader.RegisterFunction(flush_function);

	auto subscriptions_function = TableFunction("radio_subscriptions", {}, RadioSubscriptions, RadioSubscriptionsBind);
	loader.RegisterFunction(subscriptions_function);

	auto received_messages_function =
	    TableFunction("radio_received_messages", {}, RadioReceivedMessages, RadioReceivedMessagesBind);
	loader.RegisterFunction(received_messages_function);

	auto sleep_function = TableFunction("radio_sleep", {LogicalType::INTERVAL}, RadioSleep, RadioSleepBind);
	loader.RegisterFunction(sleep_function);

	RadioSubscriptionAddFunctions(loader);

	QueryFarmSendTelemetry(loader, loader.GetDatabaseInstance().shared_from_this(), "radio", "2025092301");
}

void RadioExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string RadioExtension::Name() {
	return "radio";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(radio, loader) {
	duckdb::LoadInternal(loader);
}
}
