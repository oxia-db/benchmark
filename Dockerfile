# Copyright 2025 The Oxia Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.25-bookworm AS builder
WORKDIR /app
COPY . .
RUN mkdir -p binary/build
RUN go build -o /app/binary/oxia-benchmark  ./cmd/oxia-benchmark
RUN cd /app/drivers/oxia && go build -buildmode=plugin -o /app/binary/build/driver-oxia.so .
RUN cd /app/drivers/etcd && go build -buildmode=plugin -o /app/binary/build/driver-etcd.so .
RUN cd /app/drivers/zookeeper && go build -buildmode=plugin -o /app/binary/build/driver-zookeeper.so .

FROM debian:bookworm-slim

WORKDIR /bench
COPY --from=builder /app/binary/ /bench/
ENTRYPOINT ["/bench/oxia-benchmark"]
