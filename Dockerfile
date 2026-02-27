FROM golang:1.23 AS builder
WORKDIR /tidb

COPY . .

ARG GOPROXY
ENV GOPROXY=${GOPROXY}

RUN make sync_diff_inspector

FROM rockylinux:9-minimal

COPY --from=builder /tidb/bin/sync_diff_inspector /sync_diff_inspector

WORKDIR /
