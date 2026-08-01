# Output directory: elixir_make sets MIX_APP_PATH; fall back to local priv/
PRIV_DIR ?= $(if $(MIX_APP_PATH),$(MIX_APP_PATH)/priv,priv)

# Rust NIF settings
RUST_TARGET ?= $(shell rustc -Vv 2>/dev/null | grep host | cut -d' ' -f2)
RUST_NIF_DIR = $(PRIV_DIR)/native
RUST_NIF_SO = $(RUST_NIF_DIR)/tms_engine.so
RUST_EXT_SO = $(RUST_NIF_DIR)/timeless_sqlite_ext.so
CARGO_FLAGS = --release
ifneq ($(RUST_TARGET),)
	CARGO_FLAGS += --target $(RUST_TARGET)
	RUST_OUT_DIR = native/tms_engine/target/$(RUST_TARGET)/release
	RUST_EXT_OUT_DIR = native/timeless_sqlite_ext/target/$(RUST_TARGET)/release
else
	RUST_OUT_DIR = native/tms_engine/target/release
	RUST_EXT_OUT_DIR = native/timeless_sqlite_ext/target/release
endif

.PHONY: all clean

all: $(PRIV_DIR) $(RUST_NIF_SO) $(RUST_EXT_SO)

$(PRIV_DIR):
	mkdir -p $(PRIV_DIR)

# Always rebuild — avoids stale cross-platform artifacts from Hex or other arch
$(RUST_NIF_SO): .FORCE | $(PRIV_DIR)
	mkdir -p $(RUST_NIF_DIR)
	cd native/tms_engine && cargo build $(CARGO_FLAGS)
	cp $(RUST_OUT_DIR)/libtms_engine.so $(RUST_NIF_SO) 2>/dev/null || \
	cp $(RUST_OUT_DIR)/libtms_engine.dylib $(RUST_NIF_SO) 2>/dev/null || \
	(echo "ERROR: Rust NIF build failed — cargo not found or build error" && exit 1)

$(RUST_EXT_SO): .FORCE | $(PRIV_DIR)
	mkdir -p $(RUST_NIF_DIR)
	cd native/timeless_sqlite_ext && cargo build $(CARGO_FLAGS)
	cp $(RUST_EXT_OUT_DIR)/libtimeless_sqlite_ext.so $(RUST_EXT_SO) 2>/dev/null || \
	cp $(RUST_EXT_OUT_DIR)/libtimeless_sqlite_ext.dylib $(RUST_EXT_SO) 2>/dev/null || \
	(echo "ERROR: timeless SQLite extension build failed" && exit 1)

.FORCE:

clean:
	rm -f $(RUST_NIF_SO) $(RUST_EXT_SO)
	cd native/tms_engine && cargo clean 2>/dev/null || true
	cd native/timeless_sqlite_ext && cargo clean 2>/dev/null || true
