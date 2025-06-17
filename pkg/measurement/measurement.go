// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package measurement

import (
	"fmt"
	"github.com/pingcap/go-ycsb/metrics"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type measurement struct {
	sync.RWMutex

	p *properties.Properties

	opMeasurement map[string]ycsb.Measurement
}

func (m *measurement) measure(op string, lan time.Duration) {
	m.RLock()
	opM, ok := m.opMeasurement[op]
	m.RUnlock()

	if !ok {
		opM = newHistogram(m.p)
		m.Lock()
		m.opMeasurement[op] = opM
		m.Unlock()
	}

	opM.Measure(lan)
}

func (m *measurement) output() {
	m.RLock()
	defer m.RUnlock()
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("RECOVERED")
		}
	}()

	keys := make([]string, len(m.opMeasurement))
	var i = 0
	for k := range m.opMeasurement {
		keys[i] = k
		i += 1
	}
	sort.Strings(keys)

	for _, op := range keys {
		meas, opExists := m.opMeasurement[op]
		if !opExists {
			continue
		}

		fmt.Printf("%-6s - %s\n", op, m.opMeasurement[op].Summary())
		info := meas.Info()
		qps := info.Get("QPS").(float64)
		p50 := float64(info.Get("PER50TH").(int64))
		p95 := float64(info.Get("PER95TH").(int64))
		p99 := float64(info.Get("PER99TH").(int64))

		keyPrefix := os.Getenv("KEY_PREFIX")
		if keyPrefix == "" {
			keyPrefix = "default_keyprefix"
		}

		metrics.ThroughputScope.Tagged(map[string]string{"op": op, "key_prefix": keyPrefix}).Gauge("qps").Update(qps)
		metrics.ResptimeScope.Tagged(map[string]string{"op": op, "key_prefix": keyPrefix, "quantile": "0.5"}).Gauge("time").Update(p50)
		metrics.ResptimeScope.Tagged(map[string]string{"op": op, "key_prefix": keyPrefix, "quantile": "0.95"}).Gauge("time").Update(p95)
		metrics.ResptimeScope.Tagged(map[string]string{"op": op, "key_prefix": keyPrefix, "quantile": "0.99"}).Gauge("time").Update(p99)
	}
}

func (m *measurement) info() map[string]ycsb.MeasurementInfo {
	m.RLock()
	defer m.RUnlock()

	opMeasurementInfo := make(map[string]ycsb.MeasurementInfo, len(m.opMeasurement))
	for op, opM := range m.opMeasurement {
		opMeasurementInfo[op] = opM.Info()
	}
	return opMeasurementInfo
}

func (m *measurement) getOpName() []string {
	m.RLock()
	defer m.RUnlock()

	res := make([]string, 0, len(m.opMeasurement))
	for op := range m.opMeasurement {
		res = append(res, op)
	}
	return res
}

// InitMeasure initializes the global measurement.
func InitMeasure(p *properties.Properties) {
	globalMeasure = new(measurement)
	globalMeasure.p = p
	globalMeasure.opMeasurement = make(map[string]ycsb.Measurement, 16)
	EnableWarmUp(p.GetInt64(prop.WarmUpTime, 0) > 0)
}

// Output prints the measurement summary.
func Output() {
	globalMeasure.output()
}

// EnableWarmUp sets whether to enable warm-up.
func EnableWarmUp(b bool) {
	if b {
		atomic.StoreInt32(&warmUp, 1)
	} else {
		atomic.StoreInt32(&warmUp, 0)
	}
}

// IsWarmUpFinished returns whether warm-up is finished or not.
func IsWarmUpFinished() bool {
	return atomic.LoadInt32(&warmUp) == 0
}

// Measure measures the operation.
func Measure(op string, lan time.Duration) {
	if IsWarmUpFinished() {
		globalMeasure.measure(op, lan)
	}
}

// Info returns all the operations MeasurementInfo.
// The key of returned map is the operation name.
func Info() map[string]ycsb.MeasurementInfo {
	return globalMeasure.info()
}

// GetOpNames returns a string slice which contains all the operation name measured.
func GetOpNames() []string {
	return globalMeasure.getOpName()
}

var globalMeasure *measurement
var warmUp int32 // use as bool, 1 means in warmup progress, 0 means warmup finished.
