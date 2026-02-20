FROM rust:1.90-bookworm AS builder

RUN apt-get update && apt-get install -y \
    cmake \
    librdkafka-dev \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependency builds
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Build the real application
COPY src/ src/
COPY migrations/ migrations/
RUN touch src/main.rs && cargo build --release


FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/yappa-rt /usr/local/bin/yappa-rt
COPY migrations/ /app/migrations/

WORKDIR /app
EXPOSE 8080

CMD ["yappa-rt"]