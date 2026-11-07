#pragma once

#include "duckdb.hpp"
#include "radio.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;

Radio &GetRadio(ClientContext &context);
Radio &GetRadio(DatabaseInstance &db);

// The description, examples and category shown by duckdb_functions().
FunctionDescription RadioDescribe(string description, vector<string> examples);

class RadioExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
};

} // namespace duckdb
