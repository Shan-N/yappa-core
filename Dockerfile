FROM rust:1.75-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release && rm -rf src

COPY src ./src
COPY migrations ./migrations

RUN touch src/main.rs && cargo build --release

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

RUN addgroup -S yappa && adduser -S yappa -G yappa

WORKDIR /app

COPY --from=builder /app/target/release/yappa-rt /usr/local/bin/yappa-rt

RUN chown -R yappa:yappa /app

USER yappa

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

CMD ["yappa-rt"]
