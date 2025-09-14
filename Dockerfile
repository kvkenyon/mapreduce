# Builder stage
FROM rust:latest AS builder

ARG BINARY_NAME=master

WORKDIR /app

RUN apt update && apt install lld clang -y

COPY . .

RUN cargo build --release --bin ${BINARY_NAME}

FROM debian:bookworm-slim AS runtime 

ARG BINARY_NAME=master

WORKDIR /app

RUN apt-get update -y \
  && apt-get install -y --no-install-recommends openssl ca-certificates \
  && apt-get autoremove -y \
  && apt-get clean -y \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/${BINARY_NAME} /usr/local/bin/app 

COPY .env .env 

ENTRYPOINT ["app"]
