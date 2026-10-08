PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=radio
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Exercise the websocket wire format as part of the release test suite.
ifneq ($(SKIP_TESTS),1)
test_release: test_websocket
endif

test_websocket:
	python3 "$(PROJ_DIR)test/radio_websocket_test.py" ./build/release/duckdb
