FROM --platform=$BUILDPLATFORM golang:1.22.5-alpine3.20 AS builder

ENV GOSUMDB=off

RUN go mod tidy

# Build
RUN  go build ./qubership-cql-driver
