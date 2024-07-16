FROM rust:1.79.0 AS build-env
RUN apt update \
    && apt install -y \
    clang
WORKDIR /app
COPY . /app
RUN cargo build --release

FROM gcr.io/distroless/cc
COPY --from=build-env /app/target/release/sqlite-bench /
ENTRYPOINT ["./sqlite-bench"]
CMD ["--help"]