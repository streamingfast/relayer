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
	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dmetrics"
	"github.com/dfuse-io/dstore"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/relayer"
	"github.com/dfuse-io/relayer/metrics"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

var RelayerStartAborted = fmt.Errorf("getting start block aborted by relayer application terminating signal")

type Config struct {
	SourcesAddr        []string
	GRPCListenAddr     string
	MergerAddr         string
	BufferSize         int
	MaxDrift           time.Duration
	SourceRequestBurst int
	MaxSourceLatency   time.Duration
	MinStartOffset     uint64
	InitTime           time.Duration
	SourceStoreURL     string
}

func (c *Config) ZapFields() []zap.Field {
	return []zap.Field{
		zap.Strings("sources_addr", c.SourcesAddr),
		zap.String("grpc_listen_addr", c.GRPCListenAddr),
		zap.String("merger_addr", c.MergerAddr),
		zap.Int("buffer_size", c.BufferSize),
		zap.Duration("max_drift", c.MaxDrift),
		zap.Int("source_request_burst", c.SourceRequestBurst),
		zap.Duration("max_source_latency", c.MaxSourceLatency),
		zap.Uint64("min_start_offset", c.MinStartOffset),
		zap.Duration("init_time", c.InitTime),
		zap.String("source_store_url", c.SourceStoreURL),
	}
}

type Modules struct {
	BlockFilter func(blk *bstream.Block) error
}

type App struct {
	*shutter.Shutter
	config         *Config
	modules        *Modules
	readinessProbe pbhealth.HealthClient
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
	gs, err := dgrpc.NewInternalClient(a.config.GRPCListenAddr)
	if err != nil {
		return fmt.Errorf("cannot create readiness probe")
	}
	a.readinessProbe = pbhealth.NewHealthClient(gs)

	rlayer := relayer.NewRelayer(
		a.modules.BlockFilter,
		a.config.SourcesAddr,
		a.config.MergerAddr,
		a.config.MaxSourceLatency,
		a.config.GRPCListenAddr,
		a.config.MaxDrift,
		a.config.BufferSize,
		a.config.SourceRequestBurst,
	)
	startBlockReady := make(chan uint64)
	go rlayer.PollSourceHeadUntilReady(startBlockReady, a.config.MaxSourceLatency, a.config.MinStartOffset)

	rlayer.StartListening(a.config.BufferSize)

	blocksStore, err := dstore.NewDBinStore(a.config.SourceStoreURL)
	if err != nil {
		return fmt.Errorf("getting block store: %w", err)
	}

	a.OnTerminating(rlayer.Shutdown)
	rlayer.OnTerminated(a.Shutdown)
	go rlayer.StartRelayingBlocks(startBlockReady, blocksStore, a.config.InitTime)

	return nil
}

func (a *App) IsReady() bool {
	if a.readinessProbe == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	resp, err := a.readinessProbe.Check(ctx, &pbhealth.HealthCheckRequest{})
	if err != nil {
		zlog.Info("merger readiness probe error", zap.Error(err))
		return false
	}

	if resp.Status == pbhealth.HealthCheckResponse_SERVING {
		return true
	}

	return false
}
