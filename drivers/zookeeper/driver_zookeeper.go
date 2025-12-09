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
	"fmt"
	"log/slog"
	"oxia-benchmark/drivers"
	"time"

	"github.com/go-zookeeper/zk"
)

var NewDriver = func() (drivers.KVStoreDriver, error) {
	return &ZooKeeperDriver{}, nil
}

type ZooKeeperDriver struct {
	zkc *zk.Conn
}

func (z *ZooKeeperDriver) Name() string {
	return "zookeeper"
}

func (z *ZooKeeperDriver) Init(cfg map[string]any) error {
	ss, ok := cfg["servers"].([]any)
	if !ok {
		return fmt.Errorf("invalid servers config: %v", cfg["servers"])
	}
	var servers []string
	for _, s := range ss {
		if str, ok := s.(string); ok {
			servers = append(servers, str)
		}
	}
	zkc, events, err := zk.Connect(servers, 30*time.Second)
	if err != nil {
		return err
	}
	z.zkc = zkc

	// Ensure base node exists
	if _, err = zkc.Create("/test", []byte{}, 0, zk.WorldACL(zk.PermAll)); err != nil && !errors.Is(err, zk.ErrNodeExists) {
		return err
	}

	// optional: monitor events in background
	go func() {
		for ev := range events {
			slog.Info("ZK event", "event", ev)
		}
	}()

	return nil
}

func (z *ZooKeeperDriver) Put(key string, value []byte) <-chan *drivers.KVResult {
	ch := make(chan *drivers.KVResult, 1)
	go func() {
		// interpret “key” as ZK path
		path := fmt.Sprintf("/test/%s", key)

		_, err := z.zkc.Set(path, value, -1)
		if !errors.Is(err, zk.ErrNoNode) {
			ch <- &drivers.KVResult{Err: err, End: time.Now()}
			return
		}

		// Try to create if it doesn't exist
		_, err = z.zkc.Create(path, value, 0, zk.WorldACL(zk.PermAll))
		if !errors.Is(err, zk.ErrNodeExists) {
			ch <- &drivers.KVResult{Err: err, End: time.Now()}
			return
		}
		ch <- &drivers.KVResult{Err: nil, End: time.Now()}
	}()
	return ch
}

func (z *ZooKeeperDriver) Get(key string) <-chan *drivers.KVResult {
	ch := make(chan *drivers.KVResult, 1)
	go func() {
		path := fmt.Sprintf("/test/%s", key)
		_, _, err := z.zkc.Get(path)
		if !errors.Is(err, zk.ErrNoNode) {
			ch <- &drivers.KVResult{Err: err, End: time.Now()}
		} else {
			ch <- &drivers.KVResult{Err: nil, End: time.Now()}
		}
	}()
	return ch
}

func (z *ZooKeeperDriver) Close() error {
	z.zkc.Close()
	return nil
}
