#pragma once

#include "duckdb.hpp"
#include "radio.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;

Radio &GetRadio(ClientContext &context);
Radio &GetRadio(DatabaseInstance &db);

class RadioExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
};

} // namespace duckdb
