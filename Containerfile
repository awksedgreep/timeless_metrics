# Rust 1.91+ is required by the native dependencies. Keep the toolchain in a
# throwaway stage so it never reaches the runtime image.
FROM docker.io/library/rust:1.98-bookworm AS rust-toolchain

# Stage 1: Build
FROM docker.io/hexpm/elixir:1.20.2-erlang-29.0.2-debian-bookworm-20260623 AS build

RUN apt-get update && apt-get install -y git gcc g++ make libsqlite3-dev libzstd-dev

COPY --from=rust-toolchain /usr/local/cargo /usr/local/cargo
COPY --from=rust-toolchain /usr/local/rustup /usr/local/rustup
ENV PATH="/usr/local/cargo/bin:${PATH}" \
    CARGO_HOME="/usr/local/cargo" \
    RUSTUP_HOME="/usr/local/rustup" \
    TIMELESS_BUILD_FROM_SOURCE="1"

WORKDIR /app
ENV MIX_ENV=prod

COPY mix.exs mix.lock ./
RUN mix local.hex --force && mix local.rebar --force && mix deps.get --only prod
RUN mix deps.compile

COPY lib/ lib/
COPY config/ config/
COPY rel/ rel/
COPY Makefile ./
COPY native/ native/
RUN mix release

# Stage 2: Runtime
FROM docker.io/library/debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 libzstd1 libncurses6 locales && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /data && chown 1000:1000 /data

WORKDIR /app
COPY --from=build /app/_build/prod/rel/timeless_metrics ./

USER 1000:1000
VOLUME /data
EXPOSE 8428

CMD ["bin/timeless_metrics", "start"]
