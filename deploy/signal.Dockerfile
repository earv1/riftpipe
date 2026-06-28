# Signaling server for the browser kanban — a content-blind room relay that pairs
# two peers by connection id so they can exchange WebRTC SDP. It never sees board
# data. Deploy behind TLS (fly.io / Caddy / nginx) so an HTTPS page can reach it
# over wss://. See docs/deploy-pages.md.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --bin riftpipe

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/riftpipe /usr/local/bin/riftpipe
ENV PORT=8080
ENV RIFTPIPE_BIND=0.0.0.0
EXPOSE 8080
# Binds 0.0.0.0:$PORT (TLS is terminated by the platform/proxy in front).
CMD ["sh", "-c", "riftpipe signal --port ${PORT:-8080}"]
