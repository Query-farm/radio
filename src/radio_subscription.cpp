#include "radio_extension.hpp"
#include "radio_subscription.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include "radio_utils.hpp"
#include "radio_subscription.hpp"
#include <limits>
#include <optional>

namespace duckdb {

static std::shared_ptr<RadioSubscription> RadioRequireSubscription(ClientContext &context, const std::string &url) {
	auto subscription = GetRadio(context).GetSubscription(url);
	if (!subscription) {
		throw InvalidInputException("No subscription found for URL: " + url);
	}
	return subscription;
}

static std::string RadioBindUrl(ClientContext &context, const TableFunctionBindInput &input,
                                const char *function_name) {
	return RadioRequireArgument(input, 0, function_name, "url").GetValue<string>();
}

struct RadioSubscriptionTransmitMessagesDeleteBindData : public TableFunctionData {
	explicit RadioSubscriptionTransmitMessagesDeleteBindData(const string &url, const uint64_t id)
	    : url_(url), id_(id) {
	}

	const std::string url_;
	const uint64_t id_;
};

static unique_ptr<FunctionData> RadioSubscriptionTransmitMessagesDeleteBind(ClientContext &context,
                                                                            TableFunctionBindInput &input,
                                                                            vector<LogicalType> &return_types,
                                                                            vector<Identifier> &names) {
	const char *name = "radio_subscription_transmit_message_delete";
	auto url = RadioBindUrl(context, input, name);
	auto id = RadioRequireArgument(input, 1, name, "message_id").GetValue<uint64_t>();

	return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
	names.emplace_back("ok");

	return make_uniq<RadioSubscriptionTransmitMessagesDeleteBindData>(url, id);
}

void RadioSubscriptionTransmitMessagesDelete(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<RadioSubscriptionTransmitMessagesDeleteBindData>();
	auto &state = data_p.global_state->Cast<RadioSingleRowState>();
	if (state.done) {
		output.SetChildCardinality(0);
		return;
	}
	state.done = true;

	RadioRequireSubscription(context, bind_data.url_)->transmit_messages_delete_by_ids({bind_data.id_});
	FlatVector::GetDataMutable<bool>(output.data[0])[0] = true;
	output.SetChildCardinality(1);
}

struct RadioSubscriptionUrlBindData : public TableFunctionData {
	explicit RadioSubscriptionUrlBindData(const string &url) : url_(url) {
	}

	const std::string url_;
};

static unique_ptr<FunctionData> RadioSubscriptionTransmitMessagesDeleteFinishedBind(ClientContext &context,
                                                                                    TableFunctionBindInput &input,
                                                                                    vector<LogicalType> &return_types,
                                                                                    vector<Identifier> &names) {
	auto url = RadioBindUrl(context, input, "radio_subscription_transmit_messages_delete_finished");

	return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
	names.emplace_back("ok");

	return make_uniq<RadioSubscriptionUrlBindData>(url);
}

void RadioSubscriptionTransmitMessagesDeleteFinished(ClientContext &context, TableFunctionInput &data_p,
                                                     DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<RadioSubscriptionUrlBindData>();
	auto &state = data_p.global_state->Cast<RadioSingleRowState>();
	if (state.done) {
		output.SetChildCardinality(0);
		return;
	}
	state.done = true;

	RadioRequireSubscription(context, bind_data.url_)->transmit_messages_delete_finished();
	FlatVector::GetDataMutable<bool>(output.data[0])[0] = true;
	output.SetChildCardinality(1);
}

struct RadioSubscriptionReceivedMessageAddBindData : public TableFunctionData {
	explicit RadioSubscriptionReceivedMessageAddBindData(const string &url, RadioReceivedMessage::MessageType type,
	                                                     std::optional<std::string> channel, const string &message)
	    : url_(url), message_type_(type), channel_(std::move(channel)), message_(message) {
	}

	const std::string url_;
	const RadioReceivedMessage::MessageType message_type_;
	const std::optional<std::string> channel_;
	const std::string message_;
};

static unique_ptr<FunctionData> RadioSubscriptionReceivedMessageAddBind(ClientContext &context,
                                                                        TableFunctionBindInput &input,
                                                                        vector<LogicalType> &return_types,
                                                                        vector<Identifier> &names) {
	const char *name = "radio_subscription_received_message_add";
	const auto url = RadioBindUrl(context, input, name);
	std::optional<std::string> channel;
	if (!input.inputs[1].IsNull()) {
		channel = input.inputs[1].GetValue<string>();
	}
	// Use the raw bytes; GetValue<string>() would render a BLOB with \xNN escapes.
	const auto message = StringValue::Get(RadioRequireArgument(input, 2, name, "message"));

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("message_id");

	return make_uniq<RadioSubscriptionReceivedMessageAddBindData>(url, RadioReceivedMessage::MessageType::Message,
	                                                              channel, message);
}

void RadioSubscriptionReceivedMessageAdd(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<RadioSubscriptionReceivedMessageAddBindData>();
	auto &state = data_p.global_state->Cast<RadioSingleRowState>();
	if (state.done) {
		output.SetChildCardinality(0);
		return;
	}
	state.done = true;

	auto subscription = RadioRequireSubscription(context, bind_data.url_);

	uint64_t message_id = 0;
	vector<RadioReceiveMessageParts> parts;
	parts.push_back({bind_data.message_type_, bind_data.channel_, bind_data.message_, RadioCurrentTimeMillis()});
	subscription->add_received_messages(parts, &message_id);

	FlatVector::GetDataMutable<uint64_t>(output.data[0])[0] = message_id;
	output.SetChildCardinality(1);
}

// A snapshot of one subscription's messages, taken when the query executes.
template <class T>
struct RadioMessageSnapshotState : public GlobalTableFunctionState {
	vector<std::shared_ptr<T>> messages;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> RadioSubscriptionReceivedMessagesBind(ClientContext &context,
                                                                      TableFunctionBindInput &input,
                                                                      vector<LogicalType> &return_types,
                                                                      vector<Identifier> &names) {
	auto url = RadioBindUrl(context, input, "radio_subscription_received_messages");
	// Fail early for an unknown URL; the messages are read when the query runs.
	RadioRequireSubscription(context, url);

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("message_id");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("receive_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("seen_count");

	return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
	names.emplace_back("channel");

	return_types.emplace_back(LogicalType(LogicalTypeId::BLOB));
	names.emplace_back("message");

	return make_uniq<RadioSubscriptionUrlBindData>(url);
}

static unique_ptr<GlobalTableFunctionState> RadioSubscriptionReceivedMessagesInit(ClientContext &context,
                                                                                  TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RadioSubscriptionUrlBindData>();
	auto state = make_uniq<RadioMessageSnapshotState<RadioReceivedMessage>>();
	state->messages = RadioRequireSubscription(context, bind_data.url_)->receive_snapshot();
	return std::move(state);
}

void RadioSubscriptionReceivedMessages(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<RadioMessageSnapshotState<RadioReceivedMessage>>();

	idx_t count = 0;
	while (state.offset < state.messages.size() && count < STANDARD_VECTOR_SIZE) {
		auto &message = state.messages[state.offset++];
		FlatVector::GetDataMutable<uint64_t>(output.data[0])[count] = message->id();
		FlatVector::GetDataMutable<int64_t>(output.data[1])[count] = static_cast<int64_t>(message->receive_time());
		FlatVector::GetDataMutable<uint64_t>(output.data[2])[count] = message->seen_count();
		if (message->channel().has_value()) {
			FlatVector::GetDataMutable<string_t>(output.data[3])[count] =
			    StringVector::AddStringOrBlob(output.data[3], message->channel().value());
		} else {
			FlatVector::SetNull(output.data[3], count, true);
		}
		FlatVector::GetDataMutable<string_t>(output.data[4])[count] =
		    StringVector::AddStringOrBlob(output.data[4], message->message());
		count++;
	}
	output.SetChildCardinality(count);
}

static LogicalType CreateEnumType(const string &name, const vector<string> &members) {
	Vector varchar_vector(LogicalType::VARCHAR, members.size());
	auto writer = FlatVector::Writer<string_t>(varchar_vector, members.size());
	for (idx_t i = 0; i < members.size(); i++) {
		writer.WriteValue(string_t(members[i]));
	}
	return LogicalType::ENUM(name, varchar_vector, members.size()).WithAlias(name);
}

static unique_ptr<FunctionData> RadioSubscriptionTransmitMessagesBind(ClientContext &context,
                                                                      TableFunctionBindInput &input,
                                                                      vector<LogicalType> &return_types,
                                                                      vector<Identifier> &names) {
	const auto url = RadioBindUrl(context, input, "radio_subscription_transmit_messages");
	RadioRequireSubscription(context, url);

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("message_id");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("creation_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("last_attempt_start_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("last_attempt_end_time");

	return_types.emplace_back(
	    CreateEnumType("transmit_message_state", {"pending", "sent", "sending", "retries_exhausted", "time_expired"}));
	names.emplace_back("state");

	return_types.emplace_back(LogicalType(LogicalTypeId::UINTEGER));
	names.emplace_back("expire_duration_ms");

	return_types.emplace_back(LogicalType(LogicalTypeId::UINTEGER));
	names.emplace_back("max_attempts");

	return_types.emplace_back(LogicalType(LogicalTypeId::UINTEGER));
	names.emplace_back("try_count");

	return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
	names.emplace_back("channel");

	return_types.emplace_back(LogicalType(LogicalTypeId::BLOB));
	names.emplace_back("message");

	return_types.emplace_back(LogicalType(LogicalTypeId::BLOB));
	names.emplace_back("result");

	return make_uniq<RadioSubscriptionUrlBindData>(url);
}

static unique_ptr<GlobalTableFunctionState> RadioSubscriptionTransmitMessagesInit(ClientContext &context,
                                                                                  TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RadioSubscriptionUrlBindData>();
	auto state = make_uniq<RadioMessageSnapshotState<RadioTransmitMessage>>();
	state->messages = RadioRequireSubscription(context, bind_data.url_)->transmit_snapshot();
	return std::move(state);
}

static uint8_t RadioTransmitStateEnumIndex(RadioTransmitMessageProcessingState state) {
	switch (state) {
	case RadioTransmitMessageProcessingState::PENDING:
		return 0;
	case RadioTransmitMessageProcessingState::SENT:
		return 1;
	case RadioTransmitMessageProcessingState::SENDING:
		return 2;
	case RadioTransmitMessageProcessingState::RETRIES_EXHAUSTED:
		return 3;
	case RadioTransmitMessageProcessingState::TIME_EXPIRED:
		return 4;
	}
	throw InternalException("Unknown radio transmit message state");
}

void RadioSubscriptionTransmitMessages(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<RadioMessageSnapshotState<RadioTransmitMessage>>();

	idx_t count = 0;
	while (state.offset < state.messages.size() && count < STANDARD_VECTOR_SIZE) {
		auto &message = state.messages[state.offset++];
		auto message_state = message->state();

		FlatVector::GetDataMutable<uint64_t>(output.data[0])[count] = message->id();
		FlatVector::GetDataMutable<int64_t>(output.data[1])[count] = static_cast<int64_t>(message->creation_time());
		STORE_NULLABLE_TIMESTAMP(output.data[2], count, message_state.last_attempt_start_time);
		STORE_NULLABLE_TIMESTAMP(output.data[3], count, message_state.last_attempt_end_time);
		FlatVector::GetDataMutable<uint8_t>(output.data[4])[count] = RadioTransmitStateEnumIndex(message_state.state);
		FlatVector::GetDataMutable<uint32_t>(output.data[5])[count] = message->expire_duration_ms();
		FlatVector::GetDataMutable<uint32_t>(output.data[6])[count] = message->max_attempts();
		FlatVector::GetDataMutable<uint32_t>(output.data[7])[count] = message_state.attempts_made;
		if (message->channel().has_value()) {
			FlatVector::GetDataMutable<string_t>(output.data[8])[count] =
			    StringVector::AddStringOrBlob(output.data[8], message->channel().value());
		} else {
			FlatVector::SetNull(output.data[8], count, true);
		}
		FlatVector::GetDataMutable<string_t>(output.data[9])[count] =
		    StringVector::AddStringOrBlob(output.data[9], message->message());
		FlatVector::GetDataMutable<string_t>(output.data[10])[count] =
		    StringVector::AddStringOrBlob(output.data[10], message_state.transmit_result);
		count++;
	}
	output.SetChildCardinality(count);
}

struct RadioTransmitMessageAddBindData : public TableFunctionData {
	explicit RadioTransmitMessageAddBindData(const string &url, const std::optional<std::string> &channel,
	                                         const string &message, const bool is_text, const uint32_t max_attempts,
	                                         const int64_t expire_duration_ms)
	    : url_(url), channel_(channel), message_(message), is_text_(is_text), max_attempts_(max_attempts),
	      expire_duration_ms_(expire_duration_ms) {
	}

	const std::string url_;
	const std::optional<std::string> channel_;
	const std::string message_;
	const bool is_text_;
	const uint32_t max_attempts_;
	const int64_t expire_duration_ms_;
};

static unique_ptr<FunctionData> RadioTransmitMessageAddBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<Identifier> &names) {
	const char *name = "radio_transmit_message";
	const auto url = RadioBindUrl(context, input, name);
	if (RadioSubscription::url_requires_network(url)) {
		RadioCheckExternalAccess(context, name);
	}
	std::optional<std::string> channel = std::nullopt;
	if (!input.inputs[1].IsNull()) {
		channel = input.inputs[1].GetValue<string>();
	}
	// Use the raw bytes; GetValue<string>() would render a BLOB with \xNN escapes.
	const auto &message_value = RadioRequireArgument(input, 2, name, "message");
	const auto message = StringValue::Get(message_value);
	const auto is_text = message_value.type().id() == LogicalTypeId::VARCHAR;
	const auto max_attempts = RadioRequireArgument(input, 3, name, "max_attempts").GetValue<int32_t>();
	if (max_attempts <= 0) {
		throw InvalidInputException("max_attempts must be a positive integer");
	}
	const auto expire_time = RadioRequireArgument(input, 4, name, "expire_duration").GetValue<interval_t>();

	const auto real_expire_time = Interval::GetMilli(expire_time);
	if (real_expire_time < 0 || real_expire_time > std::numeric_limits<uint32_t>::max()) {
		throw InvalidInputException("expire_duration must be between 0 and 4294967295 milliseconds");
	}

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("message_id");

	return make_uniq<RadioTransmitMessageAddBindData>(url, channel, message, is_text, max_attempts, real_expire_time);
}

void RadioTransmitMessageAdd(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<RadioTransmitMessageAddBindData>();
	auto &state = data_p.global_state->Cast<RadioSingleRowState>();
	if (state.done) {
		output.SetChildCardinality(0);
		return;
	}
	state.done = true;

	// Checked again here: a prepared statement may run after access was disabled.
	if (RadioSubscription::url_requires_network(bind_data.url_)) {
		RadioCheckExternalAccess(context, "radio_transmit_message");
	}
	auto subscription = RadioRequireSubscription(context, bind_data.url_);

	uint64_t message_id = 0;

	RadioTransmitMessageParts message_parts;
	message_parts.channel = bind_data.channel_;
	message_parts.message = bind_data.message_;
	message_parts.is_text = bind_data.is_text_;
	message_parts.expire_duration_ms = bind_data.expire_duration_ms_;
	message_parts.max_attempts = bind_data.max_attempts_;

	subscription->add_transmit_messages({message_parts}, &message_id);

	FlatVector::GetDataMutable<uint64_t>(output.data[0])[0] = message_id;
	output.SetChildCardinality(1);
}

static void RegisterSubscriptionFunction(ExtensionLoader &loader, TableFunction function, string description,
                                         vector<string> examples) {
	CreateTableFunctionInfo info(std::move(function));
	info.descriptions.push_back(RadioDescribe(std::move(description), std::move(examples)));
	loader.RegisterFunction(std::move(info));
}

void RadioSubscriptionAddFunctions(ExtensionLoader &loader) {
	// VARCHAR and BLOB payloads are both stored byte-for-byte.
	TableFunctionSet received_message_add_set("radio_subscription_received_message_add");
	for (auto &message_type : {LogicalType::VARCHAR, LogicalType::BLOB}) {
		received_message_add_set.AddFunction(TableFunction(
		    TableSignature({Positional("url", LogicalType::VARCHAR), Positional("channel", LogicalType::VARCHAR),
		                    Positional("message", message_type)}),
		    RadioSubscriptionReceivedMessageAdd, RadioSubscriptionReceivedMessageAddBind, RadioSingleRowInit));
	}
	CreateTableFunctionInfo received_message_add_info(std::move(received_message_add_set));
	received_message_add_info.descriptions.push_back(RadioDescribe(
	    "Adds a message to a subscription's receive queue as if it had arrived from the network, and returns its "
	    "message id. The channel may be NULL and the message may be VARCHAR or BLOB. Useful for testing message "
	    "handling without a server.",
	    {"SELECT * FROM radio_subscription_received_message_add('ws://localhost:8080/events', NULL, 'hello')"}));
	loader.RegisterFunction(std::move(received_message_add_info));

	RegisterSubscriptionFunction(
	    loader,
	    TableFunction("radio_subscription_transmit_messages_delete_finished",
	                  TableSignature({Positional("url", LogicalType::VARCHAR)}),
	                  RadioSubscriptionTransmitMessagesDeleteFinished,
	                  RadioSubscriptionTransmitMessagesDeleteFinishedBind, RadioSingleRowInit),
	    "Removes the transmit messages of a subscription that are finished (sent, retries exhausted or expired) and "
	    "returns true.",
	    {"SELECT * FROM radio_subscription_transmit_messages_delete_finished('ws://localhost:8080/events')"});

	RegisterSubscriptionFunction(
	    loader,
	    TableFunction("radio_subscription_transmit_message_delete",
	                  TableSignature({Positional("url", LogicalType::VARCHAR),
	                                  Positional("message_id", LogicalType::UBIGINT)}),
	                  RadioSubscriptionTransmitMessagesDelete, RadioSubscriptionTransmitMessagesDeleteBind,
	                  RadioSingleRowInit),
	    "Removes one message, by id, from a subscription's transmit queue (cancelling it if it has not been sent) "
	    "and returns true.",
	    {"SELECT * FROM radio_subscription_transmit_message_delete('ws://localhost:8080/events', 1001)"});

	RegisterSubscriptionFunction(
	    loader,
	    TableFunction("radio_subscription_received_messages", TableSignature({Positional("url", LogicalType::VARCHAR)}),
	                  RadioSubscriptionReceivedMessages, RadioSubscriptionReceivedMessagesBind,
	                  RadioSubscriptionReceivedMessagesInit),
	    "Returns the messages held in one subscription's receive queue with their id, receive time, seen count, "
	    "channel and payload. Each read increments the messages' seen_count.",
	    {"SELECT * FROM radio_subscription_received_messages('ws://localhost:8080/events')"});

	RegisterSubscriptionFunction(
	    loader,
	    TableFunction("radio_subscription_transmit_messages", TableSignature({Positional("url", LogicalType::VARCHAR)}),
	                  RadioSubscriptionTransmitMessages, RadioSubscriptionTransmitMessagesBind,
	                  RadioSubscriptionTransmitMessagesInit),
	    "Returns the messages in one subscription's transmit queue with their state (pending, sent, sending, "
	    "retries_exhausted, time_expired), attempt times and counts, and the last transport result.",
	    {"SELECT message_id, state, try_count FROM radio_subscription_transmit_messages('ws://localhost:8080/events')"});

	// VARCHAR messages are sent as WebSocket text frames, BLOB messages as binary frames.
	TableFunctionSet transmit_message_add_set("radio_transmit_message");
	for (auto &message_type : {LogicalType::VARCHAR, LogicalType::BLOB}) {
		transmit_message_add_set.AddFunction(TableFunction(
		    TableSignature({Positional("url", LogicalType::VARCHAR), Positional("channel", LogicalType::VARCHAR),
		                    Positional("message", message_type), Positional("max_attempts", LogicalType::INTEGER),
		                    Positional("expire_duration", LogicalType::INTERVAL)}),
		    RadioTransmitMessageAdd, RadioTransmitMessageAddBind, RadioSingleRowInit));
	}
	CreateTableFunctionInfo transmit_info(std::move(transmit_message_add_set));
	transmit_info.descriptions.push_back(RadioDescribe(
	    "Queues a message for sending on a subscription and returns its message id. A background sender retries "
	    "with exponential backoff until it is sent, max_attempts is reached or expire_duration passes. VARCHAR "
	    "messages are sent as WebSocket text frames, BLOB messages as binary frames; Redis subscriptions publish to "
	    "the given channel (required for Redis, ignored for WebSocket); loopback:// subscriptions receive their own "
	    "messages. Refuses network URLs when enable_external_access is false.",
	    {"SELECT * FROM radio_transmit_message('ws://localhost:8080/events', NULL, '{\"type\":\"ping\"}', 3, "
	     "INTERVAL '10 seconds')",
	     "SELECT * FROM radio_transmit_message('redis://localhost:6379?channel=alerts', 'alerts', 'disk full', 5, "
	     "INTERVAL '1 minute')"}));
	loader.RegisterFunction(std::move(transmit_info));
}

RadioSubscription::UrlType RadioSubscription::detect_url_type(const std::string &url) {
	if (url.rfind("ws://", 0) == 0 || url.rfind("wss://", 0) == 0) {
		return RadioSubscription::UrlType::WebSocket;
	} else if (url.rfind("redis://", 0) == 0 || url.rfind("redis-", 0) == 0) {
		return RadioSubscription::UrlType::Redis;
	} else if (url.rfind("loopback://", 0) == 0) {
		return RadioSubscription::UrlType::Loopback;
	}
	return RadioSubscription::UrlType::Unknown;
}

// Helper function to get query parameter from URL
static std::string get_query_param(const std::string &url, const std::string &param) {
	// Find the start of query string (after '?')
	auto pos = url.find('?');
	if (pos == std::string::npos) {
		return {};
	}

	std::string query = url.substr(pos + 1);

	size_t start = 0;
	while (start < query.size()) {
		auto end = query.find('&', start);
		if (end == std::string::npos)
			end = query.size();

		auto eq = query.find('=', start);
		if (eq != std::string::npos && eq < end) {
			std::string key = query.substr(start, eq - start);
			std::string value = query.substr(eq + 1, end - eq - 1);
			if (key == param) {
				return value;
			}
		}
		start = end + 1;
	}
	return {};
}

std::string RadioSubscription::normalize_redis_url(const std::string &url) {
	RadioSubscription::UrlType type = RadioSubscription::detect_url_type(url);
	switch (type) {
	case RadioSubscription::UrlType::Redis:
		// redis-plus-plus accepts redis:// directly. The historical Radio
		// spellings redis-tcp://, redis-tls:// and redis-unix:// are converted
		// to the corresponding redis-plus-plus schemes.
		if (url.rfind("redis-", 0) == 0) {
			return url.substr(std::string("redis-").size());
		}
		return url;
	default:
		return {};
	}
}

std::string remove_query_param(const std::string &url, const std::string &param_name) {
	std::string::size_type question_mark_pos = url.find('?');
	if (question_mark_pos == std::string::npos) {
		return url; // No query string
	}

	std::string base = url.substr(0, question_mark_pos);
	std::string query = url.substr(question_mark_pos + 1);

	std::string result;
	bool first = true;

	std::string::size_type start = 0;
	const std::string prefix = param_name + "=";

	while (start < query.size()) {
		auto end = query.find('&', start);
		if (end == std::string::npos)
			end = query.size();

		auto param = query.substr(start, end - start);
		if (param.rfind(prefix, 0) != 0) {
			if (first) {
				result += '?';
				first = false;
			} else {
				result += '&';
			}
			result += param;
		}

		start = end + 1;
	}

	return base + result;
}

RadioSubscription::RadioSubscription(const uint64_t id, const std::string &url,
                                     const RadioSubscriptionParameters &params, uint64_t creation_time, Radio &radio)
    : id_(id), url_type_(detect_url_type(url)), url_(url), creation_time_(creation_time), disabled_(false),
      received_messages_(*this, params.receive_message_capacity),
      transmit_messages_(*this, params.transmit_retry_initial_delay_ms, params.transmit_retry_multiplier,
                         params.transmit_retry_max_delay_ms),
      radio_(radio) {
}

void RadioSubscription::start() {
	try {
		if (url_type_ == UrlType::WebSocket) {
			auto websocket = std::make_unique<ix::WebSocket>();
			websocket->setUrl(url_);
			connection = std::move(websocket);

			// Install the callback before start() can emit Open/Error events.
			received_messages_.start();
			std::get<std::unique_ptr<ix::WebSocket>>(connection)->start();
			transmit_messages_.start();
			return;
		}
		if (url_type_ == UrlType::Redis) {
			auto redis_url = normalize_redis_url(url_);
			if (redis_url.empty()) {
				throw InvalidInputException("Invalid Redis URL: " + url_);
			}

			redis_url = remove_query_param(redis_url, "channel");
			auto channel_name = get_query_param(url_, "channel");
			if (channel_name.empty()) {
				throw InvalidInputException("No channel specified in Redis URL: " + url_);
			}

			sw::redis::Uri uri(redis_url);
			auto options = uri.connection_options();
			// consume() must periodically wake so shutdown can join deterministically.
			if (options.socket_timeout.count() == 0) {
				options.socket_timeout = std::chrono::milliseconds(100);
			}
			auto redis_client = std::make_unique<sw::redis::Redis>(options, uri.connection_pool_options());
			connection = RedisSubscription {std::move(redis_client), std::move(channel_name)};
			received_messages_.start();
			transmit_messages_.start();
			return;
		}
		if (url_type_ == UrlType::Loopback) {
			// No transport: transmitted messages are delivered to this
			// subscription's own receive queue by the sender thread.
			activation_time_ = RadioCurrentTimeMillis();
			transmit_messages_.start();
			return;
		}
		throw InvalidInputException("Unsupported URL type for RadioSubscription: " + url_);
	} catch (...) {
		stop();
		throw;
	}
}

void RadioSubscription::stop() noexcept {
	if (is_stopped_.exchange(true)) {
		return;
	}
	try {
		transmit_messages_.stop();
	} catch (...) {
	}
	try {
		received_messages_.stop();
	} catch (...) {
	}
	try {
		if (std::holds_alternative<std::unique_ptr<ix::WebSocket>>(connection)) {
			auto &websocket = std::get<std::unique_ptr<ix::WebSocket>>(connection);
			if (websocket) {
				websocket->stop();
			}
		}
	} catch (...) {
	}
}

RadioSubscription::~RadioSubscription() {
	stop();
}

void RadioSubscription::add_received_messages(std::vector<RadioReceiveMessageParts> messages, uint64_t *message_ids) {
	std::vector<std::shared_ptr<RadioReceivedMessage>> items;

	for (size_t i = 0; i < messages.size(); i++) {
		auto message_id = message_counter_.fetch_add(1, std::memory_order_relaxed);
		if (message_ids) {
			message_ids[i] = message_id;
		}
		auto entry =
		    std::make_shared<RadioReceivedMessage>(id_, url_, message_id, messages[i].type, messages[i].channel,
		                                           messages[i].message, messages[i].receive_time);

		items.push_back(entry);
	}
	received_messages_.push(items);

	radio_.NotifyHasMessages();
}

void RadioSubscription::add_transmit_messages(std::vector<RadioTransmitMessageParts> messages, uint64_t *message_ids) {
	std::vector<std::shared_ptr<RadioTransmitMessage>> items;

	auto current_time = RadioCurrentTimeMillis();
	for (size_t i = 0; i < messages.size(); i++) {
		auto message_id = message_counter_.fetch_add(1, std::memory_order_relaxed);
		if (message_ids) {
			message_ids[i] = message_id;
		}
		auto entry = std::make_shared<RadioTransmitMessage>(message_id, current_time, messages[i]);
		items.emplace_back(std::move(entry));
	}
	transmit_messages_.push(items);
}

} // namespace duckdb
