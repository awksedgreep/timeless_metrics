# Output directory: elixir_make sets MIX_APP_PATH; fall back to local priv/
PRIV_DIR ?= $(if $(MIX_APP_PATH),$(MIX_APP_PATH)/priv,priv)
NIF_SO = $(PRIV_DIR)/prometheus_nif.so

# Erlang NIF headers
ERTS_INCLUDE_DIR ?= $(shell erl -noshell -eval "io:format(\"~s/erts-~s/include\", [code:root_dir(), erlang:system_info(version)])." -s init stop)

# Compiler settings
CXX ?= c++
CXXFLAGS = -std=c++17 -O2 -fPIC -fvisibility=hidden -Wall -Wextra -Wno-unused-parameter
CXXFLAGS += -I$(ERTS_INCLUDE_DIR)

# Platform-specific linker flags
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	LDFLAGS = -dynamiclib -undefined dynamic_lookup
else
	LDFLAGS = -shared
endif

# Sources — put .o in PRIV_DIR so each cross-compile target gets its own
NIF_SRC = c_src/prometheus_nif.cpp
NIF_OBJ = $(PRIV_DIR)/prometheus_nif.o

.PHONY: all clean

all: $(PRIV_DIR) $(NIF_SO)

$(PRIV_DIR):
	mkdir -p $(PRIV_DIR)

# Always recompile — avoids stale cross-platform .o from Hex package or other arch
$(NIF_OBJ): .FORCE $(NIF_SRC)
	$(CXX) $(CXXFLAGS) -c -o $@ $(NIF_SRC)

$(NIF_SO): $(NIF_OBJ) | $(PRIV_DIR)
	$(CXX) $(LDFLAGS) -o $@ $(NIF_OBJ)

.FORCE:

clean:
	rm -f $(NIF_SO) $(NIF_OBJ)
