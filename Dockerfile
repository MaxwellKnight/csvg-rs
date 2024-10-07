FROM rust:1.74 as builder

WORKDIR /usr/src/app

COPY . .

RUN cargo build 

# Use a newer base image for the final stage
FROM ubuntu:22.04

# Install necessary runtime libraries
RUN apt-get update && apt-get install -y libssl3 && rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder stage
COPY --from=builder /usr/src/app/target/debug/csvg /usr/local/bin/

CMD ["csvg"]
