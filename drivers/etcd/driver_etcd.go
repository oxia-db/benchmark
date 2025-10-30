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
	"context"
	"fmt"
	"log/slog"
	"oxia-benchmark/drivers"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var NewDriver = func() (drivers.KVStoreDriver, error) {
	return &EtcdDriver{}, nil
}

type EtcdDriver struct {
	client *clientv3.Client
}

func (d *EtcdDriver) Init(cfg map[string]any) error {
	endpointsAny, ok := cfg["endpoints"]
	if !ok {
		return fmt.Errorf("missing 'endpoints' in etcd config")
	}

	var endpoints []string
	switch v := endpointsAny.(type) {
	case []any:
		for _, e := range v {
			if s, ok := e.(string); ok {
				endpoints = append(endpoints, s)
			}
		}
	case []string:
		endpoints = v
	default:
		return fmt.Errorf("invalid type for 'endpoints': %T", v)
	}

	timeout := 5 * time.Second
	if tRaw, ok := cfg["dialTimeout"]; ok {
		switch t := tRaw.(type) {
		case string:
			if dur, err := time.ParseDuration(t); err == nil {
				timeout = dur
			}
		case int:
			timeout = time.Duration(t) * time.Second
		}
	}

	var err error
	if d.client, err = clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: timeout,
		Logger:      zap.New(newSlogCore(slog.Default(), zapcore.InfoLevel)),
	}); err != nil {
		return errors.Wrap(err, "failed to connect to etcd")
	}

	return nil
}

func (d *EtcdDriver) Put(key string, value []byte) <-chan error {
	ch := make(chan error)
	go func() {
		_, err := d.client.Put(context.Background(), key, string(value))
		ch <- err
	}()
	return ch
}

func (d *EtcdDriver) Get(key string) <-chan error {
	ch := make(chan error)
	go func() {
		_, err := d.client.Get(context.Background(), key)
		ch <- err
	}()
	return ch
}

func (d *EtcdDriver) Close() error {
	return d.client.Close()
}

// ////////////////////////////////////////////////////////////////////////////////////

type slogCore struct {
	logger *slog.Logger
	level  zapcore.LevelEnabler
}

func newSlogCore(logger *slog.Logger, level zapcore.LevelEnabler) zapcore.Core {
	return &slogCore{logger: logger, level: level}
}

func (c *slogCore) Enabled(l zapcore.Level) bool {
	return c.level.Enabled(l)
}

func (c *slogCore) With(fields []zapcore.Field) zapcore.Core {
	attrs := make([]any, 0, len(fields))
	for _, f := range fields {
		attrs = append(attrs, slog.Any(f.Key, f.Interface))
	}
	return &slogCore{
		logger: c.logger.With(attrs...),
		level:  c.level,
	}
}

func (c *slogCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return checked.AddCore(entry, c)
	}
	return checked
}

func (c *slogCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	attrs := make([]slog.Attr, 0, len(fields))
	for _, f := range fields {
		attrs = append(attrs, slog.Any(f.Key, f.Interface))
	}

	var level slog.Level
	switch entry.Level {
	case zapcore.DebugLevel:
		level = slog.LevelDebug
	case zapcore.InfoLevel:
		level = slog.LevelInfo
	case zapcore.WarnLevel:
		level = slog.LevelWarn
	case zapcore.ErrorLevel:
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	c.logger.LogAttrs(context.Background(), level, entry.Message, attrs...)
	return nil
}

func (c *slogCore) Sync() error {
	return nil
}
