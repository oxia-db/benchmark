// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

module oxia-benchmark/drivers/etcd

go 1.25

require (
	github.com/go-zookeeper/zk v1.0.4
	oxia-benchmark/drivers v0.0.0-00010101000000-000000000000
)

require (
	github.com/kr/text v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace oxia-benchmark/drivers => ../
