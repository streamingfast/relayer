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

	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dstore"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/relayer"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

var RelayerStartAborted = fmt.Errorf("getting start block aborted by relayer application terminating signal")

type Config struct {
	SourcesAddr          []string
	GRPCListenAddr       string
	MergerAddr           string
	BufferSize           int
	MaxDrift             time.Duration
	MaxSourceLatency     time.Duration
	MinStartOffset       uint64
	InitTime             time.Duration
	SourceStoreURL       string
	Protocol             pbbstream.Protocol
	EnableReadinessProbe bool
}

type App struct {
	*shutter.Shutter
	config         *Config
	readinessProbe pbhealth.HealthClient
}

func New(config *Config) *App {
	return &App{
		Shutter: shutter.New(),
		config:  config,
	}
}

func (a *App) Run() error {

	if a.config.EnableReadinessProbe {
		gs, err := dgrpc.NewInternalClient(a.config.GRPCListenAddr)
		if err != nil {
			return fmt.Errorf("cannot create readiness probe")
		}
		a.readinessProbe = pbhealth.NewHealthClient(gs)
	}

	rlayer := relayer.NewRelayer(a.config.SourcesAddr, a.config.MergerAddr, a.config.MaxSourceLatency, a.config.GRPCListenAddr, a.config.MaxDrift, a.config.BufferSize)
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
