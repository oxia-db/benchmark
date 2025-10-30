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

package drivers

import (
	"fmt"
	"plugin"

	"github.com/pkg/errors"
)

func Build(conf *DriverConfig) (driver KVStoreDriver, err error) {
	switch conf.Driver {
	case "oxia", "etcd", "zookeeper":
		if driver, err = openPlugin(fmt.Sprintf("build/driver-%s.so", conf.Driver)); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown driver: %s", conf.Driver)
	}
	return driver, driver.Init(conf.Config)
}

func openPlugin(pluginPath string) (KVStoreDriver, error) {
	p, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load driver")
	}

	newDriverSymbol, err := p.Lookup("NewDriver")
	if err != nil {
		return nil, errors.Wrap(err, "failed to lookup NewDriver")
	}

	newDriverFunc := *(newDriverSymbol.(*func() (KVStoreDriver, error)))
	newDriver, err := newDriverFunc()
	if err != nil {
		return nil, errors.Wrap(err, "failed to load driver")
	}
	return newDriver, nil
}
