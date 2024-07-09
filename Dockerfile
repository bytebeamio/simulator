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

RUN apk add runit bash curl coreutils unzip

RUN mkdir -p /usr/share/bytebeam/simulator
COPY --from=builder /usr/share/bytebeam/simulator/bin /usr/bin
COPY runit/ /etc/runit
COPY data/ ./

WORKDIR /tmp/aws
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install
RUN rm -rf /tmp/aws

WORKDIR "/usr/share/bytebeam/simulator"

CMD ["/usr/bin/runsvdir", "/etc/runit"]
