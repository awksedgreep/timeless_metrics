# Stage 1: Build
FROM hexpm/elixir:1.18.3-erlang-27.3-debian-bookworm-20250428 AS build

RUN apt-get update && apt-get install -y git gcc make libsqlite3-dev libzstd-dev

WORKDIR /app
ENV MIX_ENV=prod

COPY mix.exs mix.lock ./
RUN mix local.hex --force && mix local.rebar --force && mix deps.get --only prod
RUN mix deps.compile

COPY lib/ lib/
COPY config/ config/
COPY rel/ rel/
RUN mix release

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 libzstd1 libncurses6 locales && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /data && chown 1000:1000 /data

WORKDIR /app
COPY --from=build /app/_build/prod/rel/timeless ./

USER 1000:1000
VOLUME /data
EXPOSE 8428

CMD ["bin/timeless", "start"]
