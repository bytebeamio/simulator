FROM rust:alpine AS builder

RUN apk add build-base openssl-dev
WORKDIR "/usr/share/bytebeam/simulator"

COPY src src/
COPY Cargo.* ./
COPY .git .git/

RUN mkdir -p /usr/share/bytebeam/simulator/bin
RUN cargo build --release
RUN cp target/release/simulator /usr/share/bytebeam/simulator/bin/

###################################################################################################

FROM alpine:latest

RUN apk add runit bash curl coreutils
WORKDIR "/usr/share/bytebeam/simulator"

RUN mkdir -p /usr/share/bytebeam/simulator
COPY --from=builder /usr/share/bytebeam/simulator/bin /usr/bin
COPY runit/ /etc/runit
COPY data/ ./

CMD ["/usr/bin/runsvdir", "/etc/runit"]
