// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package relayer

import (
	"context"
	"fmt"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/streamingfast/dmetrics"
	"github.com/streamingfast/dstore"
	pbhealth "github.com/streamingfast/pbgo/grpc/health/v1"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/relayer"
	"github.com/streamingfast/relayer/metrics"
	"go.uber.org/zap"
)

var RelayerStartAborted = fmt.Errorf("getting start block aborted by relayer application terminating signal")

type Config struct {
	SourcesAddr        []string
	GRPCListenAddr     string
	MergerAddr         string
	BufferSize         int
	SourceRequestBurst int
	MaxSourceLatency   time.Duration
	MinStartOffset     uint64
	SourceStoreURL     string
}

func (c *Config) ZapFields() []zap.Field {
	return []zap.Field{
		zap.Strings("sources_addr", c.SourcesAddr),
		zap.String("grpc_listen_addr", c.GRPCListenAddr),
		zap.String("merger_addr", c.MergerAddr),
		zap.Int("buffer_size", c.BufferSize),
		zap.Int("source_request_burst", c.SourceRequestBurst),
		zap.Duration("max_source_latency", c.MaxSourceLatency),
		zap.Uint64("min_start_offset", c.MinStartOffset),
		zap.String("source_store_url", c.SourceStoreURL),
	}
}

type Modules struct {
	BlockFilter func(blk *bstream.Block) error
}

type App struct {
	*shutter.Shutter
	config  *Config
	modules *Modules

	relayer *relayer.Relayer
}

func New(config *Config, modules *Modules) *App {
	return &App{
		Shutter: shutter.New(),
		config:  config,
		modules: modules,
	}
}

func (a *App) Run() error {
	dmetrics.Register(metrics.MetricSet)

	zlog.Info("starting relayer", a.config.ZapFields()...)
	a.relayer = relayer.NewRelayer(
		a.modules.BlockFilter,
		a.config.SourcesAddr,
		a.config.MergerAddr,
		a.config.MaxSourceLatency,
		a.config.GRPCListenAddr,
		a.config.BufferSize,
		a.config.SourceRequestBurst,
	)
	startBlockReady := make(chan uint64)
	go a.relayer.PollSourceHeadUntilReady(startBlockReady, a.config.MaxSourceLatency, a.config.MinStartOffset)

	a.relayer.StartListening(a.config.BufferSize)

	blocksStore, err := dstore.NewDBinStore(a.config.SourceStoreURL)
	if err != nil {
		return fmt.Errorf("getting block store: %w", err)
	}

	a.OnTerminating(a.relayer.Shutdown)
	a.relayer.OnTerminated(a.Shutdown)
	go a.relayer.StartRelayingBlocks(startBlockReady, blocksStore)

	return nil
}

var emptyHealthCheckRequest = &pbhealth.HealthCheckRequest{}

func (a *App) IsReady() bool {
	if a.relayer == nil {
		return false
	}

	resp, err := a.relayer.Check(context.Background(), emptyHealthCheckRequest)
	if err != nil {
		zlog.Info("readiness check failed", zap.Error(err))
		return false
	}

	return resp.Status == pbhealth.HealthCheckResponse_SERVING
}
