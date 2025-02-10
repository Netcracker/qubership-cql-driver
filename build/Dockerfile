FROM --platform=$BUILDPLATFORM golang:1.22.5-alpine3.20 AS builder

ENV GOSUMDB=off
WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod ./
COPY go.sum ./

RUN go mod download
RUN go mod tidy
COPY qubership-cql-driver/ ./qubership-cql-driver
# Build
RUN  go build ./qubership-cql-driver

