FROM rust:latest

RUN apt-get update && apt-get install -y \
    libzmq3-dev pkg-config \
 && rm -rf /var/lib/apt/lists/*

RUN adduser --system app
USER app
ENV USER=app

RUN cargo new --bin /home/app/iota-rdb
WORKDIR /home/app/iota-rdb
RUN rustup override set nightly

COPY Cargo.toml Cargo.lock ./
COPY iota.rs ./iota.rs/
RUN cargo build --locked --release

COPY log4rs.yaml .
COPY db ./db/
COPY src ./src/
RUN cargo build --frozen --release

ENTRYPOINT ["target/release/iota-rdb"]
CMD ["--help"]
