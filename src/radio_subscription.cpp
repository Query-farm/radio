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

struct RadioSubscriptionReceivedMessageAddBindData : public TableFunctionData {
	explicit RadioSubscriptionReceivedMessageAddBindData(Radio &radio, const string &url,
	                                                     RadioReceivedMessage::MessageType type, const string &message)
	    : radio_(radio), url_(url), message_type_(type), message_(message) {
	}

	Radio &radio_;

	const std::string url_;
	const RadioReceivedMessage::MessageType message_type_;
	const std::string message_;

	bool did_add = false;
};

static unique_ptr<FunctionData> RadioSubscriptionReceivedMessageAddBind(ClientContext &context,
                                                                        TableFunctionBindInput &input,
                                                                        vector<LogicalType> &return_types,
                                                                        vector<string> &names) {

	if (input.inputs.size() != 3) {
		throw BinderException("radio_add_message requires 3 arguments");
	}

	auto url = input.inputs[0].GetValue<string>();
	auto message_type = input.inputs[1].GetValue<string>();
	auto message = input.inputs[2].GetValue<string>();

	if (message_type != "message" && message_type != "error") {
		throw BinderException("radio_add_message requires the second argument to be either 'message' or 'error'");
	}

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("message_id");

	return make_uniq<RadioSubscriptionReceivedMessageAddBindData>(GetRadio(), url,
	                                                              message_type == "message"
	                                                                  ? RadioReceivedMessage::MessageType::MESSAGE
	                                                                  : RadioReceivedMessage::MessageType::ERROR,
	                                                              message);
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
	subscription->add_received_messages(bind_data.message_type_, {{bind_data.message_, RadioCurrentTimeMillis()}},
	                                    &message_id);

	output.SetCardinality(1);
	FlatVector::GetData<uint64_t>(output.data[0])[0] = message_id;
}

struct RadioSubscriptionReceivedMessagesBindData : public TableFunctionData {
	explicit RadioSubscriptionReceivedMessagesBindData(Radio &radio, std::shared_ptr<RadioSubscription> subscription,
	                                                   RadioReceivedMessage::MessageType message_type)
	    : radio_(radio), subscription_(subscription) {
		messages_ = subscription->receive_snapshot(message_type);
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

	if (input.inputs.size() != 2) {
		throw BinderException("radio_subscription_received_messages requires 2 arguments");
	}

	auto url = input.inputs[0].GetValue<string>();
	auto message_type = input.inputs[1].GetValue<string>();

	if (message_type != "message" && message_type != "error") {
		throw BinderException(
		    "radio_subscription_messages requires the second argument to be either 'message' or 'error'");
	}

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("message_id");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("receive_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("seen_count");

	return_types.emplace_back(LogicalType(LogicalTypeId::BLOB));
	names.emplace_back("message");

	auto subscription = GetRadio().GetSubscription(url);
	if (!subscription) {
		throw InvalidInputException("No subscription found for URL: " + url);
	}

	return make_uniq<RadioSubscriptionReceivedMessagesBindData>(GetRadio(), subscription,
	                                                            message_type == "message"
	                                                                ? RadioReceivedMessage::MessageType::MESSAGE
	                                                                : RadioReceivedMessage::MessageType::ERROR);
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
	FlatVector::GetData<string_t>(output.data[3])[0] =
	    StringVector::AddStringOrBlob(output.data[3], message->message());
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

static unique_ptr<FunctionData> RadioSubscriptionTransmitMessagesBind(ClientContext &context,
                                                                      TableFunctionBindInput &input,
                                                                      vector<LogicalType> &return_types,
                                                                      vector<string> &names) {

	if (input.inputs.size() != 1) {
		throw BinderException("radio_subscription_transmit_messages requires 1 argument");
	}

	auto url = input.inputs[0].GetValue<string>();

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("message_id");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("creation_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("last_attempt_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
	names.emplace_back("sent");

	return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP_MS));
	names.emplace_back("expire_time");

	return_types.emplace_back(LogicalType(LogicalTypeId::UINTEGER));
	names.emplace_back("max_attempts");

	return_types.emplace_back(LogicalType(LogicalTypeId::UINTEGER));
	names.emplace_back("try_count");

	return_types.emplace_back(LogicalType(LogicalTypeId::BLOB));
	names.emplace_back("message");

	auto subscription = GetRadio().GetSubscription(url);
	if (!subscription) {
		throw InvalidInputException("No subscription found for URL: " + url);
	}

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
	STORE_NULLABLE_TIMESTAMP(output.data[2], state.last_transmit_time);
	FlatVector::GetData<bool>(output.data[3])[0] = state.sent_;
	FlatVector::GetData<uint64_t>(output.data[4])[0] = message->expire_time();
	FlatVector::GetData<uint32_t>(output.data[5])[0] = message->max_attempts();
	FlatVector::GetData<uint32_t>(output.data[6])[0] = state.try_count;
	FlatVector::GetData<string_t>(output.data[7])[0] =
	    StringVector::AddStringOrBlob(output.data[7], message->message());
}

struct RadioSubscriptionTransmitMessageAddBindData : public TableFunctionData {
	explicit RadioSubscriptionTransmitMessageAddBindData(Radio &radio, const string &url, const string &message,
	                                                     const uint32_t max_attempts, const int64_t expire_time)
	    : radio_(radio), url_(url), message_(message), max_attempts_(max_attempts), expire_time_(expire_time) {
	}

	Radio &radio_;

	const std::string url_;
	const std::string message_;
	const uint32_t max_attempts_;
	const int64_t expire_time_;

	bool did_add = false;
};

static unique_ptr<FunctionData> RadioSubscriptionTransmitMessageAddBind(ClientContext &context,
                                                                        TableFunctionBindInput &input,
                                                                        vector<LogicalType> &return_types,
                                                                        vector<string> &names) {

	if (input.inputs.size() != 4) {
		throw BinderException("radio_subscription_transmit_message_add requires 4 arguments");
	}

	auto url = input.inputs[0].GetValue<string>();
	auto message = input.inputs[1].GetValue<string>();
	auto max_attempts = input.inputs[2].GetValue<uint32_t>();
	auto expire_time = input.inputs[3].GetValue<interval_t>();

	auto current_time = RadioCurrentTimeMillis();
	auto real_expire_time = current_time + Interval::GetMilli(expire_time);

	return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
	names.emplace_back("message_id");

	return make_uniq<RadioSubscriptionTransmitMessageAddBindData>(GetRadio(), url, message, max_attempts,
	                                                              real_expire_time);
}

void RadioSubscriptionTransmitMessageAdd(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioSubscriptionTransmitMessageAddBindData>();

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
	message_parts.expire_time = bind_data.expire_time_;
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

	auto received_messages_function =
	    TableFunction("radio_subscription_received_messages", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                  RadioSubscriptionReceivedMessages, RadioSubscriptionReceivedMessagesBind);
	ExtensionUtil::RegisterFunction(instance, received_messages_function);

	auto transmit_messages_function =
	    TableFunction("radio_subscription_transmit_messages", {LogicalType::VARCHAR}, RadioSubscriptionTransmitMessages,
	                  RadioSubscriptionTransmitMessagesBind);
	ExtensionUtil::RegisterFunction(instance, transmit_messages_function);

	auto transmit_message_add_function =
	    TableFunction("radio_subscription_transmit_message_add",
	                  {LogicalType::VARCHAR, LogicalType::BLOB, LogicalType::UINTEGER, LogicalType::INTERVAL},
	                  RadioSubscriptionTransmitMessageAdd, RadioSubscriptionTransmitMessageAddBind);
	ExtensionUtil::RegisterFunction(instance, transmit_message_add_function);
}

} // namespace duckdb
