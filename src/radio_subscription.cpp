
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

struct RadioSubscriptionAddMessageBindData : public TableFunctionData {
	explicit RadioSubscriptionAddMessageBindData(Radio &radio, const string &url,
	                                             RadioReceivedMessage::MessageType type, const string &message)
	    : radio_(radio), url_(url), message_type_(type), message_(message) {
	}

	Radio &radio_;

	const std::string url_;
	const RadioReceivedMessage::MessageType message_type_;
	const std::string message_;

	bool did_add = false;
};

static unique_ptr<FunctionData> RadioSubscriptionAddMessageBind(ClientContext &context, TableFunctionBindInput &input,
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

	return make_uniq<RadioSubscriptionAddMessageBindData>(GetRadio(), url,
	                                                      message_type == "message"
	                                                          ? RadioReceivedMessage::MessageType::MESSAGE
	                                                          : RadioReceivedMessage::MessageType::ERROR,
	                                                      message);
}

void RadioSubscriptionAddMessage(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioSubscriptionAddMessageBindData>();

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
	subscription->add_messages(bind_data.message_type_, {{bind_data.message_, RadioCurrentTimeMillis()}}, &message_id);

	output.SetCardinality(1);
	FlatVector::GetData<uint64_t>(output.data[0])[0] = message_id;
}

struct RadioSubscriptionMessagesBindData : public TableFunctionData {
	explicit RadioSubscriptionMessagesBindData(Radio &radio, std::shared_ptr<RadioSubscription> subscription,
	                                           RadioReceivedMessage::MessageType message_type)
	    : radio_(radio), subscription_(subscription) {
		messages_ = subscription->snapshot_messages(message_type);
	}

	Radio &radio_;
	std::shared_ptr<RadioSubscription> subscription_;

	size_t current_row = 0;
	vector<std::shared_ptr<RadioReceivedMessage>> messages_;
};

static unique_ptr<FunctionData> RadioSubscriptionMessagesBind(ClientContext &context, TableFunctionBindInput &input,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {

	if (input.inputs.size() != 2) {
		throw BinderException("radio_subscription_messages requires 2 arguments");
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

	return make_uniq<RadioSubscriptionMessagesBindData>(GetRadio(), subscription,
	                                                    message_type == "message"
	                                                        ? RadioReceivedMessage::MessageType::MESSAGE
	                                                        : RadioReceivedMessage::MessageType::ERROR);
}

void RadioSubscriptionMessages(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<RadioSubscriptionMessagesBindData>();

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

void RadioSubscriptionAddFunctions(DatabaseInstance &instance) {

	auto subscription_add_message_function =
	    TableFunction("radio_subscription_add_message", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BLOB},
	                  RadioSubscriptionAddMessage, RadioSubscriptionAddMessageBind);
	ExtensionUtil::RegisterFunction(instance, subscription_add_message_function);

	auto subscription_messages_function =
	    TableFunction("radio_subscription_messages", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                  RadioSubscriptionMessages, RadioSubscriptionMessagesBind);
	ExtensionUtil::RegisterFunction(instance, subscription_messages_function);
}

} // namespace duckdb
