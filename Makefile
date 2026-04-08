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

.PHONY: all build run docker clean lint test

all: build

build:
	./gradlew shadowJar

test:
	./gradlew test

lint:
	./gradlew spotlessCheck

run:
	java -jar build/libs/$(APP_NAME)-*-all.jar --driver-config conf/driver-oxia.yaml --workloads conf/workload-mixed.yaml

docker:
	docker build -t $(APP_NAME):latest .

clean:
	./gradlew clean
