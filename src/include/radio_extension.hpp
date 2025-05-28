#pragma once

#include "duckdb.hpp"
#include "radio.hpp"

namespace duckdb {

Radio &GetRadio();

class RadioExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	std::string Version() const override;

	Radio &GetRadio() {
		return radio_;
	}

private:
	Radio radio_;
};

} // namespace duckdb
