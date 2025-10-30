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

APP_NAME=oxia-benchmark
BUILD_DIR=build

.PHONY: all build run docker clean

all: tidy build

tidy:
	pushd drivers; go mod tidy; popd
	pushd drivers/oxia; go mod tidy; popd
	pushd drivers/etcd; go mod tidy; popd
	pushd drivers/zookeeper; go mod tidy; popd
	go mod tidy

build:
	go build -o $(BUILD_DIR)/$(APP_NAME) ./cmd/oxia-benchmark
	pushd drivers/oxia; go build -buildmode=plugin -o ../../$(BUILD_DIR)/driver-oxia.so . ; popd
	pushd drivers/etcd; go build -buildmode=plugin -o ../../$(BUILD_DIR)/driver-etcd.so . ; popd
	pushd drivers/zookeeper; go build -buildmode=plugin -o ../../$(BUILD_DIR)/driver-zookeeper.so . ; popd

run:
	$(BUILD_DIR)/$(APP_NAME) run --driver-config configs/driver_etcd.yaml --workload configs/workload_mixed.yaml

docker:
	docker build -t $(APP_NAME):latest .

clean:
	rm -rf $(BUILD_DIR)
