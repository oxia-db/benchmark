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

package runner

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Metadata struct {
	ServerNum int `yaml:"serverNum"`
}

type Workload struct {
	ReadRatio       float64       `yaml:"readRatio"`
	KeyspaceSize    uint64        `yaml:"keyspaceSize"`
	KeyDistribution string        `yaml:"keyDistribution"`
	ValueSize       int           `yaml:"valueSize"`
	TargetRate      float64       `yaml:"targetRate"`
	Duration        time.Duration `yaml:"duration"`
	Parallelism     int           `yaml:"parallelism"`
}

func (w *Workload) WithDefault() {
	if w.KeyDistribution == "" {
		w.KeyDistribution = "uniform"
	}
}

func (w *Workload) Validate() error {
	if w.KeyspaceSize == 0 {
		return fmt.Errorf("keyspaceSize must be greater than 0")
	}
	if w.ValueSize <= 0 {
		return fmt.Errorf("valueSize must be greater than 0")
	}
	if w.TargetRate <= 0 {
		return fmt.Errorf("targetRate must be greater than 0")
	}
	if w.Duration <= 0 {
		return fmt.Errorf("duration must be greater than 0")
	}
	if w.Parallelism <= 0 {
		return fmt.Errorf("parallelism must be greater than 0")
	}
	if w.ReadRatio < 0 || w.ReadRatio > 1 {
		return fmt.Errorf("readRatio must be between 0 and 1")
	}
	switch w.KeyDistribution {
	case "uniform", "zipf", "order":
	default:
		return fmt.Errorf("unsupported keyDistribution: %s", w.KeyDistribution)
	}
	return nil
}

type Workloads struct {
	Metadata       Metadata   `yaml:"metadata"`
	ExitWhenFinish bool       `yaml:"exitWhenFinish"`
	Items          []Workload `yaml:"items"`
}

func Load(path string) (*Workloads, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var w Workloads
	if err := yaml.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	for i := range w.Items {
		w.Items[i].WithDefault()
		if err := w.Items[i].Validate(); err != nil {
			return nil, fmt.Errorf("workload %d: %w", i, err)
		}
	}
	return &w, nil
}
