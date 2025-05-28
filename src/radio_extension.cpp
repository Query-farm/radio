#define DUCKDB_EXTENSION_MAIN

#include "radio_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "radio_received_message.hpp"
#include "radio_received_message_queue.hpp"
#include "radio_subscription.hpp"
#include "radio_utils.hpp"
#include "radio.hpp"

namespace duckdb {

Radio &GetRadio() {
	static Radio instance;
	return instance;
}

struct RadioSubscribeBindData : public TableFunctionData {
	explicit RadioSubscribeBindData(Radio &radio, const string &url, uint32_t max_queued_messages,
	                                uint64_t creation_time)
	    : radio_(radio), url_(url), max_queued_messages_(max_queued_messages), creation_time_(creation_time) {
	}

	Radio &radio_;
	string url_;
	uint32_t max_queued_messages_;
	uint64_t creation_time_;

	bool did_subscribe = false;
	vector<std::pair<std::shared_ptr<RadioSubscription>, std::shared_ptr<RadioReceivedMessage>>> messages_;
};

static unique_ptr<FunctionData> RadioSubscribeBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {

	auto url = input.inputs[0].GetValue<string>();
	auto max_queued_messages = input.inputs[1].GetValue<uint32_t>();
	auto creation_time = RadioCurrentTimeMillis();

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("subscription_id");

	return make_uniq<RadioSubscribeBindData>(GetRadio(), url, max_queued_messages, creation_time);
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

	auto subscription =
	    bind_data.radio_.AddSubscription(bind_data.url_, bind_data.max_queued_messages_, bind_data.creation_time_);

	FlatVector::GetData<uint64_t>(output.data[0])[0] = subscription->id();
}

struct RadioUnsubscribeBindData : public TableFunctionData {
	explicit RadioUnsubscribeBindData(Radio &radio, const string &url) : radio_(radio), url_(url) {
	}

	Radio &radio_;
	string url_;

	bool did_unsubscribe = false;
	vector<std::pair<std::shared_ptr<RadioSubscription>, std::shared_ptr<RadioReceivedMessage>>> messages_;
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

	FlatVector::GetData<uint64_t>(output.data[0])[0] = subscription->id();

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
	names.emplace_back("subscription_id");

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
			FlatVector::GetData<uint64_t>(output.data[0])[0] = subscription->id();
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

struct RadioMessagesBindData : public TableFunctionData {
	explicit RadioMessagesBindData(Radio &radio) {
		for (auto &subscription : radio.GetSubscriptions()) {
			// Add all messages for each subscription.
			for (auto &message : subscription->snapshot_messages(RadioReceivedMessage::MESSAGE)) {
				messages_.emplace_back(subscription, message);
			}
			for (auto &message : subscription->snapshot_messages(RadioReceivedMessage::ERROR)) {
				messages_.emplace_back(subscription, message);
			}
		}
	}

	size_t current_row = 0;
	vector<std::pair<std::shared_ptr<RadioSubscription>, std::shared_ptr<RadioReceivedMessage>>> messages_;
};

static unique_ptr<FunctionData> RadioMessagesBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("subscription_id");

	return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
	names.emplace_back("subscription_url");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("message_id");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("receive_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("seen_count");

	return_types.emplace_back(LogicalType(LogicalTypeId::BLOB));
	names.emplace_back("message");

	return make_uniq<RadioMessagesBindData>(GetRadio());
}

void RadioMessages(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioMessagesBindData>();

	if (bind_data.current_row >= bind_data.messages_.size()) {
		// No more messages to return.
		output.SetCardinality(0);
		return;
	}

	// Return a row at a time for now.
	auto &record = bind_data.messages_[bind_data.current_row++];
	output.SetCardinality(1);

	auto &subscription = record.first;
	auto &message = record.second;

	// From the subscription
	FlatVector::GetData<uint64_t>(output.data[0])[0] = subscription->id();
	FlatVector::GetData<string_t>(output.data[1])[0] =
	    StringVector::AddStringOrBlob(output.data[1], subscription->url());

	// From the message
	FlatVector::GetData<uint64_t>(output.data[2])[0] = message->id();
	FlatVector::GetData<uint64_t>(output.data[3])[0] = message->receive_time();
	FlatVector::GetData<uint64_t>(output.data[4])[0] = message->seen_count();
	FlatVector::GetData<string_t>(output.data[5])[0] =
	    StringVector::AddStringOrBlob(output.data[5], message->message());
}

static void LoadInternal(DatabaseInstance &instance) {

	// There are a few functions for the radio extension to process.

	// This should take an optional parameter for max number of messages.

	auto subscribe_function = TableFunction("radio_subscribe", {LogicalType::VARCHAR, LogicalType::UINTEGER},
	                                        RadioSubscribe, RadioSubscribeBind);
	ExtensionUtil::RegisterFunction(instance, subscribe_function);

	auto unsubscribe_function =
	    TableFunction("radio_unsubscribe", {LogicalType::VARCHAR}, RadioUnsubscribe, RadioUnsubscribeBind);
	ExtensionUtil::RegisterFunction(instance, unsubscribe_function);

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

	// Add a message just like if it was received from a subscription.

	auto listen_function =
	    TableFunction("radio_listen", {LogicalType::BOOLEAN, LogicalType::DOUBLE}, RadioListen, RadioListenBind);
	ExtensionUtil::RegisterFunction(instance, listen_function);

	auto subscriptions_function = TableFunction("radio_subscriptions", {}, RadioSubscriptions, RadioSubscriptionsBind);
	ExtensionUtil::RegisterFunction(instance, subscriptions_function);

	auto messages_function = TableFunction("radio_messages", {}, RadioMessages, RadioMessagesBind);
	ExtensionUtil::RegisterFunction(instance, messages_function);

	RadioSubscriptionAddFunctions(instance);
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
