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

package main

import (
	"errors"
	"log/slog"
	"oxia-benchmark/drivers"
	"time"

	"github.com/oxia-db/oxia/oxia"
)

var NewDriver = func() (drivers.KVStoreDriver, error) {
	return &OxiaDriver{}, nil
}

type OxiaDriver struct {
	client oxia.AsyncClient
}

func (o *OxiaDriver) Init(conf map[string]any) error {
	slog.Info("Init Oxia Driver",
		slog.Any("conf", conf),
	)

	var clientOptions []oxia.ClientOption

	if ns, ok := conf["namespace"].(string); ok {
		clientOptions = append(clientOptions, oxia.WithNamespace(ns))
	}

	if batchLinger, ok := conf["batchLinger"].(string); ok {
		td, err := time.ParseDuration(batchLinger)
		if err != nil {
			return err
		}
		clientOptions = append(clientOptions, oxia.WithBatchLinger(td))
	}

	if bmc, ok := conf["batchMaxCount"].(int); ok {
		clientOptions = append(clientOptions, oxia.WithMaxRequestsPerBatch(bmc))
	}

	var err error
	if o.client, err = oxia.NewAsyncClient("localhost:6648", clientOptions...); err != nil {
		return err
	}

	return nil
}

func (o *OxiaDriver) Put(key string, value []byte) <-chan *drivers.KVResult {
	chRes := o.client.Put(key, value)
	chErr := make(chan *drivers.KVResult)

	go func() {
		pr := <-chRes
		chErr <- &drivers.KVResult{Err: pr.Err, End: time.Now()}
		close(chErr)
	}()

	return chErr
}

func (o *OxiaDriver) Get(key string) <-chan *drivers.KVResult {
	chRes := o.client.Get(key)
	chErr := make(chan *drivers.KVResult)

	go func() {
		pr := <-chRes
		if errors.Is(pr.Err, oxia.ErrKeyNotFound) {
			chErr <- &drivers.KVResult{Err: nil, End: time.Now()}
		} else {
			chErr <- &drivers.KVResult{Err: pr.Err, End: time.Now()}
		}
		close(chErr)
	}()

	return chErr
}

func (o *OxiaDriver) Close() error {
	return o.client.Close()
}
