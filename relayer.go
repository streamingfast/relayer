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
	"strings"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/blockstream"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/bstream/hub"
	"github.com/streamingfast/dgrpc"
	"github.com/streamingfast/relayer/metrics"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
	pbhealth "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	getHeadInfoTimeout = 10 * time.Second
)

type Relayer struct {
	*shutter.Shutter

	grpcListenAddr         string
	bufferSize             int
	liveSourceFactory      bstream.SourceFactory
	oneBlocksSourceFactory bstream.SourceFromNumFactory

	hub       *hub.ForkableHub
	hubSource bstream.Source

	ready bool

	blockStreamServer *blockstream.Server
}

func NewRelayer(
	liveSourceFactory bstream.SourceFactory,
	oneBlocksSourceFactory bstream.SourceFromNumFactory,
	grpcListenAddr string,
	bufferSize int) *Relayer {
	r := &Relayer{
		Shutter:                shutter.New(),
		grpcListenAddr:         grpcListenAddr,
		liveSourceFactory:      liveSourceFactory,
		oneBlocksSourceFactory: oneBlocksSourceFactory,
	}

	gs := dgrpc.NewServer()
	pbhealth.RegisterHealthServer(gs, r)
	r.blockStreamServer = blockstream.NewBufferedServer(gs, bufferSize, blockstream.ServerOptionWithLogger(zlog))
	r.StartListening(bufferSize)
	return r

}

func NewMultiplexedSource(handler bstream.Handler, sourceAddresses []string, maxSourceLatency time.Duration, sourceRequestBurst int) bstream.Source {
	ctx := context.Background()

	var sourceFactories []bstream.SourceFactory
	for _, url := range sourceAddresses {

		sourceName := urlToLoggerName(url)
		logger := zlog.Named("src").Named(sourceName)
		sf := func(subHandler bstream.Handler) bstream.Source {

			gate := bstream.NewRealtimeGate(maxSourceLatency, subHandler, bstream.GateOptionWithLogger(logger))
			var upstreamHandler bstream.Handler
			upstreamHandler = bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
				return gate.ProcessBlock(blk, &namedObj{
					Obj:  obj,
					Name: sourceName,
				})
			})

			src := blockstream.NewSource(ctx, url, int64(sourceRequestBurst), upstreamHandler, blockstream.WithLogger(logger), blockstream.WithRequester("relayer"))
			return src
		}
		sourceFactories = append(sourceFactories, sf)
	}

	return bstream.NewMultiplexedSource(sourceFactories, handler, bstream.MultiplexedSourceWithLogger(zlog))
}

func urlToLoggerName(url string) string {
	return strings.TrimPrefix(strings.TrimPrefix(url, "dns:///"), ":")
}

func (r *Relayer) Run() {
	forkableHub := hub.NewForkableHub(
		r.liveSourceFactory,
		r.oneBlocksSourceFactory,
		110,
		forkable.EnsureAllBlocksTriggerLongestChain(), // send every forked block too
		forkable.WithFilters(bstream.StepNew),
	)
	go forkableHub.Run()
	zlog.Info("waiting for hub to be ready...")
	<-forkableHub.Ready

	handler := bstream.HandlerFunc(func(blk *bstream.Block, _ interface{}) error {
		zlog.Debug("publishing block", zap.Stringer("block", blk))

		metrics.HeadBlockTimeDrift.SetBlockTime(blk.Time())
		metrics.HeadBlockNumber.SetUint64(blk.Num())

		return r.blockStreamServer.PushBlock(blk)
	})

	for {
		zlog.Info("getting a block source from hub...")
		if src := forkableHub.SourceFromBlockNum(forkableHub.LowestBlockNum(), handler); src != nil {
			r.hubSource = src
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	r.OnTerminating(func(e error) {
		zlog.Info("shutting down source")
		r.hubSource.Shutdown(e)

		zlog.Info("closing block stream server")
		r.blockStreamServer.Close()
	})

	zlog.Info("Relayer started")
	r.ready = true
	r.hubSource.Run()

	err := r.hubSource.Err()
	zlog.Debug("shutting down relayer because source was shut down", zap.Error(err))
	r.Shutdown(err)
}

type namedObj struct {
	Name string
	Obj  interface{}
}
