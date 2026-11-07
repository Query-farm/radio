#define DUCKDB_EXTENSION_MAIN

#include "radio_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>

#include "radio_received_message.hpp"
#include "radio_received_message_queue.hpp"
#include "radio_subscription.hpp"
#include "radio_utils.hpp"
#include "radio.hpp"
#include "radio_version.hpp"

#include "query_farm_telemetry.hpp"

#include <ixwebsocket/IXNetSystem.h>

namespace duckdb {

Radio &GetRadio(DatabaseInstance &db) {
	auto radio = db.GetObjectCache().GetOrCreate<Radio>("radio");
	if (!radio) {
		throw InternalException("Could not initialize database-local Radio state");
	}
	return *radio;
}

Radio &GetRadio(ClientContext &context) {
	return GetRadio(DatabaseInstance::GetDatabase(context));
}

FunctionDescription RadioDescribe(string description, vector<string> examples) {
	FunctionDescription desc;
	desc.description = std::move(description);
	desc.examples = std::move(examples);
	desc.categories = {"messaging"};
	return desc;
}

static LogicalType CreateEnumType(const string &name, const vector<string> &members) {
	Vector varchar_vector(LogicalType::VARCHAR, members.size());
	auto writer = FlatVector::Writer<string_t>(varchar_vector, members.size());
	for (idx_t i = 0; i < members.size(); i++) {
		writer.WriteValue(string_t(members[i]));
	}
	return LogicalType::ENUM(name, varchar_vector, members.size()).WithAlias(name);
}

struct RadioSubscribeBindData : public TableFunctionData {
	explicit RadioSubscribeBindData(const string &url, RadioSubscriptionParameters &params)
	    : url_(url), params_(params) {
	}

	string url_;
	RadioSubscriptionParameters params_;
};

static unique_ptr<FunctionData> RadioSubscribeBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto url = RadioRequireArgument(input, 0, "radio_subscribe", "url").GetValue<string>();
	if (RadioSubscription::url_requires_network(url)) {
		RadioCheckExternalAccess(context, "radio_subscribe");
	}

	RadioSubscriptionParameters params;

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first.GetIdentifierName());
		if (kv.second.IsNull()) {
			throw BinderException("radio_subscribe requires %s to be non-NULL", loption);
		}
		if (loption == "receive_message_capacity") {
			params.receive_message_capacity = kv.second.GetValue<int32_t>();
			if (params.receive_message_capacity <= 0) {
				throw BinderException("radio_subscribe requires receive_message_capacity to be greater than 0");
			}
		} else if (loption == "transmit_retry_initial_delay_ms") {
			params.transmit_retry_initial_delay_ms = kv.second.GetValue<int32_t>();
			// A zero initial delay would stay zero after every backoff multiplication
			// and retry a failing transport in a tight loop.
			if (params.transmit_retry_initial_delay_ms <= 0) {
				throw BinderException("radio_subscribe requires transmit_retry_initial_delay_ms to be greater than 0");
			}
		} else if (loption == "transmit_retry_multiplier") {
			params.transmit_retry_multiplier = kv.second.GetValue<double>();
			if (!std::isfinite(params.transmit_retry_multiplier) || params.transmit_retry_multiplier <= 1.0) {
				throw BinderException(
				    "radio_subscribe requires transmit_retry_multiplier to be finite and greater than 1.0");
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

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("subscription_id");

	return make_uniq<RadioSubscribeBindData>(url, params);
}

void RadioSubscribe(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<RadioSubscribeBindData>();
	auto &state = data_p.global_state->Cast<RadioSingleRowState>();
	if (state.done) {
		output.SetChildCardinality(0);
		return;
	}
	state.done = true;

	// Checked again here: a prepared statement may run after access was disabled.
	if (RadioSubscription::url_requires_network(bind_data.url_)) {
		RadioCheckExternalAccess(context, "radio_subscribe");
	}
	auto subscription = GetRadio(context).AddSubscription(bind_data.url_, bind_data.params_, RadioCurrentTimeMillis());

	FlatVector::GetDataMutable<uint64_t>(output.data[0])[0] = subscription->id();
	output.SetChildCardinality(1);
}

struct RadioUrlBindData : public TableFunctionData {
	explicit RadioUrlBindData(const string &url) : url_(url) {
	}

	string url_;
};

static unique_ptr<FunctionData> RadioUnsubscribeBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto url = RadioRequireArgument(input, 0, "radio_unsubscribe", "url").GetValue<string>();

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("subscription_id");

	return make_uniq<RadioUrlBindData>(url);
}

void RadioUnsubscribe(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<RadioUrlBindData>();
	auto &state = data_p.global_state->Cast<RadioSingleRowState>();
	if (state.done) {
		output.SetChildCardinality(0);
		return;
	}
	state.done = true;

	auto &radio = GetRadio(context);
	auto subscription = radio.GetSubscription(bind_data.url_);
	if (!subscription) {
		throw InvalidInputException("No subscription found for URL: " + bind_data.url_);
	}
	radio.RemoveSubscription(subscription->id());

	FlatVector::GetDataMutable<uint64_t>(output.data[0])[0] = subscription->id();
	output.SetChildCardinality(1);
}

// The subscriptions a scan returns, captured when the query executes (not
// when it is bound) so that prepared statements see current state.
struct RadioSubscriptionListState : public GlobalTableFunctionState {
	std::vector<std::shared_ptr<RadioSubscription>> subscriptions;
	idx_t offset = 0;
	bool computed = false;
};

static unique_ptr<GlobalTableFunctionState> RadioSubscriptionListInit(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return make_uniq<RadioSubscriptionListState>();
}

void RadioSubscriptions(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<RadioSubscriptionListState>();
	if (!state.computed) {
		state.subscriptions = GetRadio(context).GetSubscriptions();
		state.computed = true;
	}

	idx_t count = 0;
	while (state.offset < state.subscriptions.size() && count < STANDARD_VECTOR_SIZE) {
		auto &subscription = state.subscriptions[state.offset++];
		auto queue_state = subscription->state();

		FlatVector::GetDataMutable<uint64_t>(output.data[0])[count] = subscription->id();
		FlatVector::GetDataMutable<string_t>(output.data[1])[count] =
		    StringVector::AddStringOrBlob(output.data[1], subscription->url());
		FlatVector::GetDataMutable<int64_t>(output.data[2])[count] =
		    static_cast<int64_t>(subscription->creation_time());
		STORE_NULLABLE_TIMESTAMP(output.data[3], count, subscription->activation_time());
		FlatVector::GetDataMutable<bool>(output.data[4])[count] = subscription->disabled();
		STORE_NULLABLE_TIMESTAMP(output.data[5], count, queue_state.received.latest_receive_time);
		FlatVector::GetDataMutable<uint64_t>(output.data[6])[count] = queue_state.received.odometer;
		FlatVector::GetDataMutable<uint64_t>(output.data[7])[count] = queue_state.transmit.odometer;
		STORE_NULLABLE_TIMESTAMP(output.data[8], count, queue_state.transmit.latest_queue_time);
		STORE_NULLABLE_TIMESTAMP(output.data[9], count, queue_state.transmit.latest_success_time);
		STORE_NULLABLE_TIMESTAMP(output.data[10], count, queue_state.transmit.latest_failure_time);
		FlatVector::GetDataMutable<uint64_t>(output.data[11])[count] = queue_state.transmit.successes;
		FlatVector::GetDataMutable<uint64_t>(output.data[12])[count] = queue_state.transmit.failures;
		FlatVector::GetDataMutable<uint64_t>(output.data[13])[count] = subscription->receive_queue_size();
		FlatVector::GetDataMutable<uint64_t>(output.data[14])[count] = queue_state.received.dropped_unseen;
		count++;
	}
	output.SetChildCardinality(count);
}

static unique_ptr<FunctionData> RadioSubscriptionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
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

	// The number of received messages currently held in the receive queue.
	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("received_messages_queued");

	// Received messages evicted from the full receive queue before any query read them.
	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("received_messages_dropped_unseen");

	return make_uniq<TableFunctionData>();
}

struct RadioIntervalBindData : public TableFunctionData {
	explicit RadioIntervalBindData(int64_t duration_ms) : duration_ms(duration_ms) {
	}

	const int64_t duration_ms;
};

static int64_t RadioBindNonNegativeInterval(const TableFunctionBindInput &input, idx_t index,
                                            const char *function_name, const char *parameter_name) {
	auto duration = RadioRequireArgument(input, index, function_name, parameter_name).GetValue<interval_t>();
	auto milliseconds = Interval::GetMilli(duration);
	if (milliseconds < 0) {
		throw BinderException("%s requires %s to be a non-negative interval", function_name, parameter_name);
	}
	return milliseconds;
}

static unique_ptr<FunctionData> RadioSleepBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto duration_ms = RadioBindNonNegativeInterval(input, 0, "radio_sleep", "duration");

	return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
	names.emplace_back("finished");

	return make_uniq<RadioIntervalBindData>(duration_ms);
}

void RadioSleep(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<RadioIntervalBindData>();
	auto &state = data_p.global_state->Cast<RadioSingleRowState>();
	if (state.done) {
		output.SetChildCardinality(0);
		return;
	}
	state.done = true;

	RadioInterruptibleSleepUntil(context,
	                             std::chrono::steady_clock::now() + std::chrono::milliseconds(bind_data.duration_ms));

	FlatVector::GetDataMutable<bool>(output.data[0])[0] = true;
	output.SetChildCardinality(1);
}

static unique_ptr<FunctionData> RadioFlushBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto timeout_ms = RadioBindNonNegativeInterval(input, 0, "radio_flush", "timeout");

	return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
	names.emplace_back("all_messages_flushed");

	return make_uniq<RadioIntervalBindData>(timeout_ms);
}

void RadioFlush(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<RadioIntervalBindData>();
	auto &state = data_p.global_state->Cast<RadioSingleRowState>();
	if (state.done) {
		output.SetChildCardinality(0);
		return;
	}
	state.done = true;

	const auto finish_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(bind_data.duration_ms);

	bool all_flushed = true;
	for (auto &subscription : GetRadio(context).GetSubscriptions()) {
		// Wait in short slices so the query can be interrupted.
		while (true) {
			RadioCheckInterrupted(context);
			const auto now = std::chrono::steady_clock::now();
			const auto slice_end = std::min(finish_time, now + RADIO_INTERRUPT_POLL_INTERVAL);
			if (subscription->flush_complete(slice_end)) {
				break;
			}
			if (slice_end >= finish_time) {
				all_flushed = false;
				break;
			}
		}
		if (!all_flushed) {
			break;
		}
	}

	FlatVector::GetDataMutable<bool>(output.data[0])[0] = all_flushed;
	output.SetChildCardinality(1);
}

struct RadioListenBindData : public TableFunctionData {
	RadioListenBindData(bool wait_for_messages, int64_t wait_timeout_ms)
	    : wait_for_messages(wait_for_messages), wait_timeout_ms(wait_timeout_ms) {
	}

	const bool wait_for_messages;
	const int64_t wait_timeout_ms;
};

static unique_ptr<FunctionData> RadioListenBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto wait_for_messages = RadioRequireArgument(input, 0, "radio_listen", "wait_for_messages").GetValue<bool>();
	auto timeout_ms = RadioBindNonNegativeInterval(input, 1, "radio_listen", "timeout");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("subscription_id");

	return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
	names.emplace_back("subscription_url");

	return make_uniq<RadioListenBindData>(wait_for_messages, timeout_ms);
}

static std::vector<std::shared_ptr<RadioSubscription>> RadioSubscriptionsWithUnseenMessages(Radio &radio) {
	std::vector<std::shared_ptr<RadioSubscription>> result;
	for (auto &subscription : radio.GetSubscriptions()) {
		if (subscription->has_unseen()) {
			result.push_back(subscription);
		}
	}
	return result;
}

void RadioListen(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<RadioListenBindData>();
	auto &state = data_p.global_state->Cast<RadioSubscriptionListState>();

	if (!state.computed) {
		state.computed = true;
		auto &radio = GetRadio(context);
		const auto deadline =
		    std::chrono::steady_clock::now() + std::chrono::milliseconds(bind_data.wait_timeout_ms);
		while (true) {
			// Read the generation before scanning: a message that arrives while
			// scanning bumps it, so the wait below returns at once instead of
			// sleeping for the full timeout.
			const auto generation = radio.MessageGeneration();
			state.subscriptions = RadioSubscriptionsWithUnseenMessages(radio);
			if (!state.subscriptions.empty() || !bind_data.wait_for_messages) {
				break;
			}
			const auto now = std::chrono::steady_clock::now();
			if (now >= deadline) {
				break;
			}
			const auto slice = std::min<std::chrono::steady_clock::duration>(deadline - now,
			                                                                 RADIO_INTERRUPT_POLL_INTERVAL);
			radio.WaitForMessages(generation, std::chrono::duration_cast<std::chrono::milliseconds>(slice) +
			                                      std::chrono::milliseconds(1));
			RadioCheckInterrupted(context);
		}
	}

	idx_t count = 0;
	while (state.offset < state.subscriptions.size() && count < STANDARD_VECTOR_SIZE) {
		auto &subscription = state.subscriptions[state.offset++];
		FlatVector::GetDataMutable<uint64_t>(output.data[0])[count] = subscription->id();
		FlatVector::GetDataMutable<string_t>(output.data[1])[count] =
		    StringVector::AddStringOrBlob(output.data[1], subscription->url());
		count++;
	}
	output.SetChildCardinality(count);
}

struct RadioReceivedMessagesState : public GlobalTableFunctionState {
	vector<std::shared_ptr<RadioReceivedMessage>> messages;
	idx_t offset = 0;
};

static unique_ptr<GlobalTableFunctionState> RadioReceivedMessagesInit(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto state = make_uniq<RadioReceivedMessagesState>();
	// Snapshot when the query executes; taking the snapshot marks the
	// messages as seen.
	for (auto &subscription : GetRadio(context).GetSubscriptions()) {
		for (auto &message : subscription->receive_snapshot()) {
			state->messages.emplace_back(message);
		}
	}
	return std::move(state);
}

static unique_ptr<FunctionData> RadioReceivedMessagesBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types,
                                                          vector<Identifier> &names) {
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

	return make_uniq<TableFunctionData>();
}

void RadioReceivedMessages(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<RadioReceivedMessagesState>();

	idx_t count = 0;
	while (state.offset < state.messages.size() && count < STANDARD_VECTOR_SIZE) {
		auto &message = state.messages[state.offset++];

		FlatVector::GetDataMutable<uint64_t>(output.data[0])[count] = message->subscription_id();
		FlatVector::GetDataMutable<string_t>(output.data[1])[count] =
		    StringVector::AddStringOrBlob(output.data[1], message->subscription_url());
		FlatVector::GetDataMutable<uint64_t>(output.data[2])[count] = message->id();
		FlatVector::GetDataMutable<uint8_t>(output.data[3])[count] =
		    RadioReceivedMessage::message_type_to_enum_index(message->type());
		FlatVector::GetDataMutable<int64_t>(output.data[4])[count] = static_cast<int64_t>(message->receive_time());
		FlatVector::GetDataMutable<uint64_t>(output.data[5])[count] = message->seen_count();
		if (message->channel().has_value()) {
			FlatVector::GetDataMutable<string_t>(output.data[6])[count] =
			    StringVector::AddStringOrBlob(output.data[6], *message->channel());
		} else {
			FlatVector::SetNull(output.data[6], count, true);
		}
		FlatVector::GetDataMutable<string_t>(output.data[7])[count] =
		    StringVector::AddStringOrBlob(output.data[7], message->message());
		count++;
	}
	output.SetChildCardinality(count);
}

inline void RadioVersionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result.SetValue(0, Value(RADIO_EXTENSION_VERSION));
}

static void RegisterRadioTableFunction(ExtensionLoader &loader, TableFunction function, string description,
                                       vector<string> examples) {
	CreateTableFunctionInfo info(std::move(function));
	info.descriptions.push_back(RadioDescribe(std::move(description), std::move(examples)));
	loader.RegisterFunction(std::move(info));
}

static void LoadInternal(ExtensionLoader &loader) {
	// Database-local state is destroyed by DatabaseInstance::object_cache.reset(),
	// before extension dependencies can be unloaded.
	GetRadio(loader.GetDatabaseInstance());

	// Initializes Winsock on Windows (reference counted); a no-op elsewhere.
	ix::initNetSystem();

	auto subscribe_function =
	    TableFunction("radio_subscribe", TableSignature({Positional("url", LogicalType::VARCHAR)}), RadioSubscribe,
	                  RadioSubscribeBind, RadioSingleRowInit);
	// Optional named parameters: a call that leaves one out does not pass it.
	subscribe_function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("receive_message_capacity", LogicalType::INTEGER)
		    .Add("transmit_retry_initial_delay_ms", LogicalType::INTEGER)
		    .Add("transmit_retry_multiplier", LogicalType::DOUBLE)
		    .Add("transmit_retry_max_delay_ms", LogicalType::INTEGER);
	});
	RegisterRadioTableFunction(
	    loader, std::move(subscribe_function),
	    "Subscribes to a WebSocket (ws://, wss://) or Redis pub/sub (redis://host:port?channel=name) URL and returns "
	    "the subscription id. A loopback://name URL needs no server: messages transmitted on it are received by the "
	    "same subscription. The connection is made in the background; subscribing to a URL that is already "
	    "subscribed returns the existing subscription. Optional named parameters: receive_message_capacity (default "
	    "1000; the oldest message is dropped when the receive queue is full), transmit_retry_initial_delay_ms "
	    "(default 100), transmit_retry_multiplier (default 1.5) and transmit_retry_max_delay_ms (default 10000). "
	    "Refuses network URLs when enable_external_access is false.",
	    {"SELECT * FROM radio_subscribe('ws://localhost:8080/events')", "SELECT * FROM radio_subscribe('loopback://test')",
	     "SELECT * FROM radio_subscribe('redis://localhost:6379?channel=alerts', receive_message_capacity := 10000)"});

	RegisterRadioTableFunction(
	    loader,
	    TableFunction("radio_unsubscribe", TableSignature({Positional("url", LogicalType::VARCHAR)}),
	                  RadioUnsubscribe, RadioUnsubscribeBind, RadioSingleRowInit),
	    "Closes the subscription for a URL, discarding its received and queued transmit messages, and returns the "
	    "subscription id. Raises an error if the URL is not subscribed.",
	    {"SELECT * FROM radio_unsubscribe('ws://localhost:8080/events')"});

	{
		auto version_function = ScalarFunction("radio_version", {}, LogicalType::VARCHAR, RadioVersionFunction);
		CreateScalarFunctionInfo info(std::move(version_function));
		info.descriptions.push_back(
		    RadioDescribe("Returns the version of the radio extension (YYYYMMDDNN).", {"radio_version()"}));
		loader.RegisterFunction(std::move(info));
	}

	RegisterRadioTableFunction(
	    loader,
	    TableFunction("radio_listen",
	                  TableSignature({Positional("wait_for_messages", LogicalType::BOOLEAN),
	                                  Positional("timeout", LogicalType::INTERVAL)}),
	                  RadioListen, RadioListenBind, RadioSubscriptionListInit),
	    "Returns the subscriptions (id and URL) that hold received messages no query has read yet. When "
	    "wait_for_messages is true and there are none, blocks until a message arrives or the timeout passes; the "
	    "wait can be interrupted. Reading the messages, e.g. with radio_received_messages(), marks them as seen.",
	    {"SELECT * FROM radio_listen(true, INTERVAL '10 seconds')", "SELECT * FROM radio_listen(false, INTERVAL '0 seconds')"});

	RegisterRadioTableFunction(
	    loader,
	    TableFunction("radio_flush", TableSignature({Positional("timeout", LogicalType::INTERVAL)}), RadioFlush,
	                  RadioFlushBind, RadioSingleRowInit),
	    "Waits up to the timeout for every subscription's transmit queue to finish sending (or give up on) its "
	    "pending messages. Returns true if all were flushed in time, false otherwise.",
	    {"SELECT * FROM radio_flush(INTERVAL '5 seconds')"});

	RegisterRadioTableFunction(
	    loader, TableFunction("radio_subscriptions", {}, RadioSubscriptions, RadioSubscriptionsBind,
	                          RadioSubscriptionListInit),
	    "Lists the active subscriptions with their URL, creation and activation time, and receive/transmit queue "
	    "statistics, including received_messages_queued and received_messages_dropped_unseen (messages evicted from "
	    "a full receive queue before any query read them).",
	    {"SELECT * FROM radio_subscriptions()",
	     "SELECT url, received_messages_dropped_unseen FROM radio_subscriptions()"});

	RegisterRadioTableFunction(
	    loader, TableFunction("radio_received_messages", {}, RadioReceivedMessages, RadioReceivedMessagesBind,
	                          RadioReceivedMessagesInit),
	    "Returns the messages held in the receive queues of all subscriptions: message, connection, disconnection "
	    "and error events, with their receive time, channel and payload. Each read increments the messages' "
	    "seen_count.",
	    {"SELECT * FROM radio_received_messages()",
	     "SELECT message::VARCHAR FROM radio_received_messages() WHERE message_type = 'message'"});

	RegisterRadioTableFunction(
	    loader,
	    TableFunction("radio_sleep", TableSignature({Positional("duration", LogicalType::INTERVAL)}), RadioSleep,
	                  RadioSleepBind, RadioSingleRowInit),
	    "Sleeps for the given interval and returns true; the sleep can be interrupted. Useful in scripts that "
	    "wait for messages to arrive.",
	    {"SELECT * FROM radio_sleep(INTERVAL '1 second')"});

	RadioSubscriptionAddFunctions(loader);

	QueryFarmSendTelemetry(loader, "radio", RADIO_EXTENSION_VERSION);
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
