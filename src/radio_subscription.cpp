#include "radio_extension.hpp"
#include "radio_subscription.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "radio_utils.hpp"
#include "radio_subscription.hpp"

namespace duckdb {

struct RadioSubscriptionTransmitMessagesDeleteBindData : public TableFunctionData {
	explicit RadioSubscriptionTransmitMessagesDeleteBindData(Radio &radio, const string &url, const uint64_t id)
	    : radio_(radio), url_(url), id_(id) {
	}

	Radio &radio_;

	const std::string url_;
	const uint64_t id_;
	bool did_delete = false;
};

static unique_ptr<FunctionData> RadioSubscriptionTransmitMessagesDeleteBind(ClientContext &context,
                                                                            TableFunctionBindInput &input,
                                                                            vector<LogicalType> &return_types,
                                                                            vector<string> &names) {

	if (input.inputs.size() != 2) {
		throw BinderException("radio_subscription_transmit_message_delete requires 2 arguments");
	}

	auto url = input.inputs[0].GetValue<string>();
	auto id = input.inputs[1].GetValue<uint64_t>();

	return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
	names.emplace_back("ok");

	return make_uniq<RadioSubscriptionTransmitMessagesDeleteBindData>(GetRadio(), url, id);
}

void RadioSubscriptionTransmitMessagesDelete(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioSubscriptionTransmitMessagesDeleteBindData>();

	if (bind_data.did_delete) {
		// If we already added the message, just return no rows.
		output.SetCardinality(0);
		return;
	}

	bind_data.did_delete = true;

	auto subscription = bind_data.radio_.GetSubscription(bind_data.url_);
	if (!subscription) {
		throw InvalidInputException("No subscription found for URL: " + bind_data.url_);
	}

	subscription->transmit_messages_delete_by_ids({bind_data.id_});
	output.SetCardinality(1);
	FlatVector::GetData<bool>(output.data[0])[0] = true;
}

struct RadioSubscriptionTransmitMessagesDeleteFinishedBindData : public TableFunctionData {
	explicit RadioSubscriptionTransmitMessagesDeleteFinishedBindData(Radio &radio, const string &url)
	    : radio_(radio), url_(url) {
	}

	Radio &radio_;

	const std::string url_;
	bool did_delete = false;
};

static unique_ptr<FunctionData> RadioSubscriptionTransmitMessagesDeleteFinishedBind(ClientContext &context,
                                                                                    TableFunctionBindInput &input,
                                                                                    vector<LogicalType> &return_types,
                                                                                    vector<string> &names) {

	if (input.inputs.size() != 1) {
		throw BinderException("radio_subscription_transmit_messages_delete_finished requires 1 argument");
	}

	auto url = input.inputs[0].GetValue<string>();

	return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
	names.emplace_back("ok");

	return make_uniq<RadioSubscriptionTransmitMessagesDeleteFinishedBindData>(GetRadio(), url);
}

void RadioSubscriptionTransmitMessagesDeleteFinished(ClientContext &context, TableFunctionInput &data_p,
                                                     DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioSubscriptionTransmitMessagesDeleteFinishedBindData>();

	if (bind_data.did_delete) {
		// If we already added the message, just return no rows.
		output.SetCardinality(0);
		return;
	}

	bind_data.did_delete = true;

	auto subscription = bind_data.radio_.GetSubscription(bind_data.url_);
	if (!subscription) {
		throw InvalidInputException("No subscription found for URL: " + bind_data.url_);
	}

	subscription->transmit_messages_delete_finished();
	output.SetCardinality(1);
	FlatVector::GetData<bool>(output.data[0])[0] = true;
}

struct RadioSubscriptionReceivedMessageAddBindData : public TableFunctionData {
	explicit RadioSubscriptionReceivedMessageAddBindData(Radio &radio, const string &url,
	                                                     RadioReceivedMessage::MessageType type, const string &channel,
	                                                     const string &message)
	    : radio_(radio), url_(url), message_type_(type), channel_(channel), message_(message) {
	}

	Radio &radio_;

	const std::string url_;
	const RadioReceivedMessage::MessageType message_type_;
	const std::string channel_;
	const std::string message_;

	bool did_add = false;
};

static unique_ptr<FunctionData> RadioSubscriptionReceivedMessageAddBind(ClientContext &context,
                                                                        TableFunctionBindInput &input,
                                                                        vector<LogicalType> &return_types,
                                                                        vector<string> &names) {

	if (input.inputs.size() != 3) {
		throw BinderException("radio_add_message requires 4 arguments");
	}

	const auto url = input.inputs[0].GetValue<string>();
	const auto message_type = input.inputs[1].GetValue<string>();
	const auto channel = input.inputs[2].GetValue<string>();
	const auto message = input.inputs[3].GetValue<string>();

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("message_id");

	return make_uniq<RadioSubscriptionReceivedMessageAddBindData>(
	    GetRadio(), url, RadioReceivedMessage::convert_to_message_type(message_type), channel, message);
}

void RadioSubscriptionReceivedMessageAdd(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioSubscriptionReceivedMessageAddBindData>();

	if (bind_data.did_add) {
		// If we already added the message, just return no rows.
		output.SetCardinality(0);
		return;
	}
	bind_data.did_add = true;

	auto subscription = bind_data.radio_.GetSubscription(bind_data.url_);
	if (!subscription) {
		throw InvalidInputException("No subscription found for URL: " + bind_data.url_);
	}

	uint64_t message_id = 0;
	vector<RadioReceiveMessageParts> parts;
	parts.push_back({bind_data.message_type_, bind_data.channel_, bind_data.message_, RadioCurrentTimeMillis()});
	subscription->add_received_messages(parts, &message_id);

	output.SetCardinality(1);
	FlatVector::GetData<uint64_t>(output.data[0])[0] = message_id;
}

struct RadioSubscriptionReceivedMessagesBindData : public TableFunctionData {
	explicit RadioSubscriptionReceivedMessagesBindData(Radio &radio, std::shared_ptr<RadioSubscription> subscription)
	    : radio_(radio), subscription_(subscription) {
		messages_ = subscription->receive_snapshot();
	}

	Radio &radio_;
	std::shared_ptr<RadioSubscription> subscription_;

	size_t current_row = 0;
	vector<std::shared_ptr<RadioReceivedMessage>> messages_;
};

static unique_ptr<FunctionData> RadioSubscriptionReceivedMessagesBind(ClientContext &context,
                                                                      TableFunctionBindInput &input,
                                                                      vector<LogicalType> &return_types,
                                                                      vector<string> &names) {

	if (input.inputs.size() != 1) {
		throw BinderException("radio_subscription_received_messages requires 1 argument");
	}

	auto url = input.inputs[0].GetValue<string>();

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

	auto subscription = GetRadio().GetSubscription(url);
	if (!subscription) {
		throw InvalidInputException("No subscription found for URL: " + url);
	}

	return make_uniq<RadioSubscriptionReceivedMessagesBindData>(GetRadio(), subscription);
}

void RadioSubscriptionReceivedMessages(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioSubscriptionReceivedMessagesBindData>();

	if (bind_data.current_row >= bind_data.messages_.size()) {
		// No more messages to return.
		output.SetCardinality(0);
		return;
	}

	// Return a row at a time for now.
	auto &message = bind_data.messages_[bind_data.current_row++];
	output.SetCardinality(1);

	FlatVector::GetData<uint64_t>(output.data[0])[0] = message->id();
	FlatVector::GetData<uint64_t>(output.data[1])[0] = message->receive_time();
	FlatVector::GetData<uint64_t>(output.data[2])[0] = message->seen_count();

	if (message->channel().has_value()) {
		FlatVector::GetData<string_t>(output.data[3])[0] =
		    StringVector::AddStringOrBlob(output.data[3], message->channel().value());
	} else {
		FlatVector::SetNull(output.data[3], 0, true);
	}
	FlatVector::GetData<string_t>(output.data[4])[0] =
	    StringVector::AddStringOrBlob(output.data[4], message->message());
}

struct RadioSubscriptionTransmitMessagesBindData : public TableFunctionData {
	explicit RadioSubscriptionTransmitMessagesBindData(Radio &radio, std::shared_ptr<RadioSubscription> subscription)
	    : radio_(radio), subscription_(subscription) {
		messages_ = subscription->transmit_snapshot();
	}

	Radio &radio_;
	std::shared_ptr<RadioSubscription> subscription_;

	size_t current_row = 0;
	vector<std::shared_ptr<RadioTransmitMessage>> messages_;
};

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

static unique_ptr<FunctionData> RadioSubscriptionTransmitMessagesBind(ClientContext &context,
                                                                      TableFunctionBindInput &input,
                                                                      vector<LogicalType> &return_types,
                                                                      vector<string> &names) {

	if (input.inputs.size() != 1) {
		throw BinderException("radio_subscription_transmit_messages requires 1 argument");
	}

	const auto url = input.inputs[0].GetValue<string>();
	auto subscription = GetRadio().GetSubscription(url);
	if (!subscription) {
		throw InvalidInputException("No subscription found for URL: " + url);
	}

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

	return make_uniq<RadioSubscriptionTransmitMessagesBindData>(GetRadio(), subscription);
}

void RadioSubscriptionTransmitMessages(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioSubscriptionTransmitMessagesBindData>();

	if (bind_data.current_row >= bind_data.messages_.size()) {
		// No more messages to return.
		output.SetCardinality(0);
		return;
	}

	// Return a row at a time for now.
	auto &message = bind_data.messages_[bind_data.current_row++];
	output.SetCardinality(1);

	auto state = message->state();

	FlatVector::GetData<uint64_t>(output.data[0])[0] = message->id();
	FlatVector::GetData<uint64_t>(output.data[1])[0] = message->creation_time();
	STORE_NULLABLE_TIMESTAMP(output.data[2], state.last_attempt_start_time);
	STORE_NULLABLE_TIMESTAMP(output.data[3], state.last_attempt_end_time);

	int16_t state_idx;
	switch (state.state) {
	case RadioTransmitMessageProcessingState::PENDING:
		state_idx = 0;
		break;
	case RadioTransmitMessageProcessingState::SENT:
		state_idx = 1;
		break;
	case RadioTransmitMessageProcessingState::SENDING:
		state_idx = 2;
		break;
	case RadioTransmitMessageProcessingState::RETRIES_EXHAUSTED:
		state_idx = 3;
		break;
	case RadioTransmitMessageProcessingState::TIME_EXPIRED:
		state_idx = 4;
		break;
	}
	FlatVector::GetData<int16_t>(output.data[4])[0] = state_idx;

	FlatVector::GetData<uint32_t>(output.data[5])[0] = message->expire_duration_ms();
	FlatVector::GetData<uint32_t>(output.data[6])[0] = message->max_attempts();
	FlatVector::GetData<uint32_t>(output.data[7])[0] = state.attempts_made;

	if (message->channel().has_value()) {
		FlatVector::GetData<string_t>(output.data[8])[0] =
		    StringVector::AddStringOrBlob(output.data[8], message->channel().value());
	} else {
		FlatVector::SetNull(output.data[8], 0, true);
	}

	FlatVector::GetData<string_t>(output.data[9])[0] =
	    StringVector::AddStringOrBlob(output.data[9], message->message());
	FlatVector::GetData<string_t>(output.data[10])[0] =
	    StringVector::AddStringOrBlob(output.data[10], state.transmit_result);
}

struct RadioTransmitMessageAddBindData : public TableFunctionData {
	explicit RadioTransmitMessageAddBindData(Radio &radio, const string &url, const std::optional<std::string> &channel,
	                                         const string &message, const uint32_t max_attempts,
	                                         const int64_t expire_duration_ms)
	    : radio_(radio), url_(url), channel_(channel), message_(message), max_attempts_(max_attempts),
	      expire_duration_ms_(expire_duration_ms) {
	}

	Radio &radio_;

	const std::string url_;
	const std::optional<std::string> channel_;
	const std::string message_;
	const uint32_t max_attempts_;
	const int64_t expire_duration_ms_;

	bool did_add = false;
};

static unique_ptr<FunctionData> RadioTransmitMessageAddBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {

	if (input.inputs.size() != 5) {
		throw BinderException("radio_transmit_message requires 5 arguments");
	}

	const auto url = input.inputs[0].GetValue<string>();
	std::optional<std::string> channel = std::nullopt;
	if (!input.inputs[1].IsNull()) {
		channel = input.inputs[1].GetValue<string>();
	}
	const auto message = input.inputs[2].GetValue<string>();
	const auto max_attempts = input.inputs[3].GetValue<int32_t>();
	if (max_attempts <= 0) {
		throw InvalidInputException("max_attempts must be a positive integer");
	}
	const auto expire_time = input.inputs[4].GetValue<interval_t>();

	const auto real_expire_time = Interval::GetMilli(expire_time);

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("message_id");

	return make_uniq<RadioTransmitMessageAddBindData>(GetRadio(), url, channel, message, max_attempts,
	                                                  real_expire_time);
}

void RadioTransmitMessageAdd(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioTransmitMessageAddBindData>();

	if (bind_data.did_add) {
		// If we already added the message, just return no rows.
		output.SetCardinality(0);
		return;
	}
	bind_data.did_add = true;

	auto subscription = bind_data.radio_.GetSubscription(bind_data.url_);
	if (!subscription) {
		throw InvalidInputException("No subscription found for URL: " + bind_data.url_);
	}

	uint64_t message_id = 0;

	RadioTransmitMessageParts message_parts;
	message_parts.message = bind_data.message_;
	message_parts.expire_duration_ms = bind_data.expire_duration_ms_;
	message_parts.max_attempts = bind_data.max_attempts_;

	subscription->add_transmit_messages({message_parts}, &message_id);

	output.SetCardinality(1);
	FlatVector::GetData<uint64_t>(output.data[0])[0] = message_id;
}

void RadioSubscriptionAddFunctions(DatabaseInstance &instance) {

	auto received_message_add_function = TableFunction(
	    "radio_subscription_received_message_add", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BLOB},
	    RadioSubscriptionReceivedMessageAdd, RadioSubscriptionReceivedMessageAddBind);
	ExtensionUtil::RegisterFunction(instance, received_message_add_function);

	auto transmit_messages_delete_finished = TableFunction(
	    "radio_subscription_transmit_messages_delete_finished", {LogicalType::VARCHAR},
	    RadioSubscriptionTransmitMessagesDeleteFinished, RadioSubscriptionTransmitMessagesDeleteFinishedBind);
	ExtensionUtil::RegisterFunction(instance, transmit_messages_delete_finished);

	auto transmit_messages_delete =
	    TableFunction("radio_subscription_transmit_message_delete", {LogicalType::VARCHAR, LogicalType::UBIGINT},
	                  RadioSubscriptionTransmitMessagesDelete, RadioSubscriptionTransmitMessagesDeleteBind);
	ExtensionUtil::RegisterFunction(instance, transmit_messages_delete);

	auto received_messages_function =
	    TableFunction("radio_subscription_received_messages", {LogicalType::VARCHAR}, RadioSubscriptionReceivedMessages,
	                  RadioSubscriptionReceivedMessagesBind);
	ExtensionUtil::RegisterFunction(instance, received_messages_function);

	auto transmit_messages_function =
	    TableFunction("radio_subscription_transmit_messages", {LogicalType::VARCHAR}, RadioSubscriptionTransmitMessages,
	                  RadioSubscriptionTransmitMessagesBind);
	ExtensionUtil::RegisterFunction(instance, transmit_messages_function);

	auto transmit_message_add_function = TableFunction(
	    "radio_transmit_message",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BLOB, LogicalType::INTEGER, LogicalType::INTERVAL},
	    RadioTransmitMessageAdd, RadioTransmitMessageAddBind);
	ExtensionUtil::RegisterFunction(instance, transmit_message_add_function);
}

RadioSubscription::UrlType RadioSubscription::detect_url_type(const std::string &url) {
	if (url.rfind("ws://", 0) == 0) {
		return RadioSubscription::UrlType::WebSocket;
	} else if (url.rfind("redis-", 0) == 0) {
		return RadioSubscription::UrlType::Redis;
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
		// Convert redis-tcp:// to tcp://
		return url.substr(std::string("redis-").size());
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
    : id_(id), url_type_(detect_url_type(url)), url_(std::move(url)), creation_time_(creation_time), disabled_(false),
      received_messages_(*this, params.receive_message_capacity),
      transmit_messages_(*this, params.transmit_retry_initial_delay_ms, params.transmit_retry_multiplier,
                         params.transmit_retry_max_delay_ms),
      radio_(radio) {
}

void RadioSubscription::start() {
	if (url_type_ == UrlType::WebSocket) {
		auto webSocket = std::make_unique<ix::WebSocket>();
		webSocket->setUrl(url_);
		webSocket->start();
		connection = std::move(webSocket);
	} else if (url_type_ == UrlType::Redis) {
		auto redis_url = normalize_redis_url(url_);
		if (redis_url.empty()) {
			throw InvalidInputException("Invalid Redis URL: " + url_);
		}

		redis_url = remove_query_param(redis_url, "channel");

		auto parsed_channel_name_ = get_query_param(url_, "channel");
		if (parsed_channel_name_.empty()) {
			throw InvalidInputException("No channel specified in Redis URL: " + url_);
		}

		auto redis_client = std::make_unique<sw::redis::Redis>(redis_url);

		this->activation_time_ = RadioCurrentTimeMillis();
		connection = RedisSubscription {std::move(redis_client), nullptr, parsed_channel_name_};
	} else {
		throw InvalidInputException("Unsupported URL type for RadioSubscription: " + url_);
	}
	this->transmit_messages_.start();
	this->received_messages_.start();
}

void RadioSubscription::stop() {
	if (is_stopped_) {
		return;
	}
	is_stopped_ = true;
	transmit_messages_.stop();
	received_messages_.stop();
	if (std::holds_alternative<std::unique_ptr<ix::WebSocket>>(connection)) {
		auto &webSocket = std::get<std::unique_ptr<ix::WebSocket>>(connection);
		webSocket->stop();
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
		auto entry = std::make_shared<RadioReceivedMessage>(*this, message_id, messages[i].type, messages[i].channel,
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
