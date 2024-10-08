# Build stage
# TODO: move this over to alpine
FROM golang:1.23.1 AS builder

# Install dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy go.mod and go.sum to cache dependencies
COPY go.mod .
COPY go.sum .

# Download Go modules
RUN go mod download

# Copy the rest of the source code
COPY *.go ./
COPY ./cmd ./cmd
COPY ./sources ./sources
COPY ./sinks ./sinks
COPY ./server ./server
COPY ./utils ./utils
COPY ./pipeline ./pipeline
COPY ./internal ./internal
COPY ./.config ./.config
COPY ./.config/config.json ./.config/config.json
COPY ./.config/config.yaml ./.config/config.yaml

# Build the application binary
RUN go build -o /wire ./cmd

# Final stage - slim image
FROM debian:bullseye-slim

# Set up the work directory
WORKDIR /app

# Copy only the built binary from the previous stage
COPY --from=builder /wire /wire

# Copy the config folder
COPY ./.config ./.config

# Define the default config path using an environment variable
# ENV CONFIG_PATH=./.config/config.json

# Command to run the application with the config file argument
CMD [ "/wire", "--config", "./.config/config.json" ]

