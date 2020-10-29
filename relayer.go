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
	"strings"
	"sync"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/blockstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dstore"
	pbheadinfo "github.com/dfuse-io/pbgo/dfuse/headinfo/v1"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/relayer/metrics"
	"github.com/dfuse-io/shutter"
	"github.com/golang/protobuf/ptypes"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Relayer struct {
	*shutter.Shutter

	blockFilter          func(blk *bstream.Block) error
	sourceAddresses      []string
	mergerAddr           string
	ready                bool
	source               bstream.Source
	restartEternalSource func()
	maxSourceLatency     time.Duration
	sourceRequestBurst   int64
	grpcListenAddr       string
	blockStreamServer    *blockstream.Server

	lastBlockMutex          sync.Mutex
	lastBlockSentTime       time.Time
	highestSentBlockRef     bstream.BlockRef // use this cause eternalsource does not see *after* the forkable
	highestReceivedBlockNum uint64           // use this to compare if highestReceivedBlockNum>lastSentBlockRef by more than 1, then maxWaitTime is considered...
}

func NewRelayer(blockFilter func(blk *bstream.Block) error, sourceAddresses []string, mergerAddr string, maxSourceLatency time.Duration, grpcListenAddr string, bufferSize, sourceRequestBurst int) *Relayer {
	r := &Relayer{
		Shutter:             shutter.New(),
		blockFilter:         blockFilter,
		sourceAddresses:     sourceAddresses,
		mergerAddr:          mergerAddr,
		maxSourceLatency:    maxSourceLatency,
		grpcListenAddr:      grpcListenAddr,
		sourceRequestBurst:  int64(sourceRequestBurst),
		highestSentBlockRef: bstream.BlockRefEmpty,
	}

	gs := dgrpc.NewServer()
	pbhealth.RegisterHealthServer(gs, r)
	r.blockStreamServer = blockstream.NewBufferedServer(gs, bufferSize, blockstream.ServerOptionWithLogger(zlog))
	return r

}

func calculateDesiredStartBlock(head, lib uint64, minOffsetToHead uint64) uint64 {
	if head-lib > minOffsetToHead {
		if lib > minOffsetToHead {
			return lib
		}
		return bstream.GetProtocolFirstStreamableBlock
	}
	if head > minOffsetToHead {
		return head - minOffsetToHead
	}
	return bstream.GetProtocolFirstStreamableBlock
}

func (r *Relayer) PollSourceHeadUntilReady(readyStartBlock chan uint64, maxSourceLatency time.Duration, minOffsetToHead uint64) {
	defer close(readyStartBlock)

	sleepTime := 0 * time.Second
	logCounter := 0

	for {
		select {
		case <-time.After(sleepTime):
			sleepTime = 500 * time.Millisecond
		case <-r.Terminating():
			return
		}

		headInfo := r.fetchHighestHeadInfo()
		if headInfo == nil {
			if logCounter%5 == 0 {
				zlog.Info("cannot get head info mindreader, retrying forever", zap.Any("sources_addr", r.sourceAddresses))
			}
			continue
		}

		headTime, err := ptypes.Timestamp(headInfo.HeadTime)
		if err != nil {
			zlog.Error("invalid headtime retrieved from upstread headinfo")
			continue
		}

		r.blockStreamServer.SetHeadInfo(headInfo.HeadNum, headInfo.HeadID, headTime, headInfo.LibNum)
		r.ready = true

		observedLatency := time.Since(headTime)
		if observedLatency < maxSourceLatency {
			readyStartBlock <- calculateDesiredStartBlock(headInfo.HeadNum, headInfo.LibNum, minOffsetToHead)
			return
		} else {
			if logCounter%5 == 0 {
				zlog.Info("source head latency too high", zap.Any("sources_addr", r.sourceAddresses), zap.Duration("max_source_latency", maxSourceLatency), zap.Duration("observed_latency", observedLatency))
			}
		}
	}
}

func (r *Relayer) fetchHighestHeadInfo() (headInfo *pbheadinfo.HeadInfoResponse) {
	for _, addr := range r.sourceAddresses {
		conn, err := dgrpc.NewInternalClient(addr)
		if err != nil {
			zlog.Info("cannot connect to backend", zap.String("address", addr))
			continue
		}

		headinfoCli := pbheadinfo.NewHeadInfoClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		remoteHead, err := headinfoCli.GetHeadInfo(ctx, &pbheadinfo.HeadInfoRequest{}, grpc.WaitForReady(false))
		if err != nil || remoteHead == nil {
			zlog.Info("cannot get headinfo from backend", zap.String("address", addr), zap.Error(err))
			continue
		}
		if headInfo == nil || remoteHead.HeadNum > headInfo.HeadNum {
			headInfo = remoteHead
		}
	}

	return
}

func (r *Relayer) SetupBlockStreamServer(bufferSize int) {
	gs := dgrpc.NewServer()
	pbhealth.RegisterHealthServer(gs, r)
	r.blockStreamServer = blockstream.NewBufferedServer(gs, bufferSize, blockstream.ServerOptionWithLogger(zlog))
}

func (r *Relayer) blockHoleDetected() bool {
	r.lastBlockMutex.Lock()
	defer r.lastBlockMutex.Unlock()
	if time.Since(r.lastBlockSentTime) < 5*time.Second { // allows small inconsistencies when reading initial buffers from different sources
		return false
	}
	return r.highestSentBlockRef.Num() != 0 && r.highestReceivedBlockNum > (r.highestSentBlockRef.Num()+1)
}

func (r *Relayer) resetBlockHoleMonitoring() {
	r.lastBlockMutex.Lock()
	defer r.lastBlockMutex.Unlock()
	r.highestReceivedBlockNum = r.highestSentBlockRef.Num() //  we cheat a bit here
}

func (r *Relayer) newMultiplexedSource(handler bstream.Handler) bstream.Source {
	ctx := context.Background()

	var sourceFactories []bstream.SourceFactory
	for _, url := range r.sourceAddresses {
		u := url // https://github.com/golang/go/wiki/CommonMistakes

		logger := zlog.Named("src").Named(urlToLoggerName(u))
		sf := func(subHandler bstream.Handler) bstream.Source {
			gate := bstream.NewRealtimeGate(r.maxSourceLatency, subHandler, bstream.GateOptionWithLogger(logger))

			upstreamHandler := bstream.Handler(gate)
			if r.blockFilter != nil {
				// When the block filter is present, we use it to filter block received from the source. We put
				// it at the nearest point of received blocks so blocks flowing in-process's memory are lightweight.
				upstreamHandler = bstream.NewPreprocessor(func(blk *bstream.Block) (interface{}, error) {
					return nil, r.blockFilter(blk)
				}, gate)
			}

			src := blockstream.NewSource(ctx, u, r.sourceRequestBurst, upstreamHandler, blockstream.WithLogger(logger), blockstream.WithRequester("relayer"))
			return src
		}
		sourceFactories = append(sourceFactories, sf)
	}

	return bstream.NewMultiplexedSource(sourceFactories, handler, bstream.MultiplexedSourceWithLogger(zlog))
}

func urlToLoggerName(url string) string {
	return strings.TrimPrefix(url, ":")
}

func (r *Relayer) StartRelayingBlocks(startBlockReady chan uint64, blockStore dstore.Store) {
	/*

			   Graph:

		       [----------------------]
		       [    JoiningSource     ]
		       [----------------------]
		       |  - FileSource        ]
		       |  - MultiplexedSource ]
		       |----------------------]
		                    |
		       [-----------------------------]
		       [  BlockNumGate               ]
		       [  (startBlockNum, inclusive) ]
		       [-----------------------------]
		                    |
		       [-------------------------]
		       [ Forkable                ]
		       [   - only StepNew        ]
		       [   - trigger all blocks  ]
		       [-------------------------]
		                    |
		       [-------------------------]
		       [   PushToServer handler  ]
		       [-------------------------]

	*/

	var startBlock uint64
	select {
	case <-r.Terminating():
		return
	case startBlock = <-startBlockReady:
	}

	pipe := bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
		zlog.Debug("publishing block", zap.Stringer("block", blk))

		r.lastBlockMutex.Lock()
		if blk.Num() > r.highestSentBlockRef.Num() {
			r.highestSentBlockRef = bstream.NewBlockRef(blk.ID(), blk.Num())
			r.lastBlockSentTime = time.Now()
		}
		r.lastBlockMutex.Unlock()

		metrics.HeadBlockTimeDrift.SetBlockTime(blk.Time())
		metrics.HeadBlockNumber.SetUint64(blk.Num())

		return r.blockStreamServer.PushBlock(blk)
	})

	forkableHandler := forkable.New(pipe,
		forkable.WithLogger(zlog),
		forkable.WithFilters(forkable.StepNew),
		forkable.EnsureAllBlocksTriggerLongestChain(),
	)

	var filterPreprocessFunc bstream.PreprocessFunc
	if r.blockFilter != nil {
		filterPreprocessFunc = func(blk *bstream.Block) (interface{}, error) {
			return nil, r.blockFilter(blk)
		}
	}

	sf := bstream.SourceFromRefFactory(func(startBlockRef bstream.BlockRef, h bstream.Handler) bstream.Source {

		jsOptions := []bstream.JoiningSourceOption{
			bstream.JoiningSourceLogger(zlog),
			bstream.JoiningSourceMergerAddr(r.mergerAddr),
			bstream.JoiningSourceTargetBlockNum(bstream.GetProtocolFirstStreamableBlock),
			bstream.JoiningSourceLiveTracker(200, r.sourcesBlockRefGetter()),
		}

		if startBlockRef != bstream.BlockRefEmpty {
			startBlock = startBlockRef.Num()
			jsOptions = append(jsOptions, bstream.JoiningSourceTargetBlockID(startBlockRef.ID()))
		}

		gate := bstream.NewBlockNumGate(startBlock, bstream.GateInclusive, h, bstream.GateOptionWithLogger(zlog))

		fileSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
			return bstream.NewFileSource(blockStore, startBlock, 2, filterPreprocessFunc, subHandler)
		})

		zlog.Info("new joining source with", zap.Uint64("start_block_num", startBlock))
		js := bstream.NewJoiningSource(fileSourceFactory, r.newMultiplexedSource, gate, jsOptions...)
		go r.monitorBlockHole(func() { js.Shutdown(fmt.Errorf("hole detected in blocks")) }) // triggers eternalsource restart
		return js
	})

	h := bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
		r.lastBlockMutex.Lock()
		if blk.Number > r.highestReceivedBlockNum {
			r.highestReceivedBlockNum = blk.Number
		}
		r.lastBlockMutex.Unlock()
		return forkableHandler.ProcessBlock(blk, obj)
	})
	r.source = bstream.NewDelegatingEternalSource(sf, func() (bstream.BlockRef, error) { return r.highestSentBlockRef, nil }, h, bstream.EternalSourceWithLogger(zlog))

	r.OnTerminating(func(e error) {
		zlog.Info("shutting down source")
		r.source.Shutdown(e)

		zlog.Info("closing block stream server")
		r.blockStreamServer.Close()
	})

	r.source.Run()

	err := r.source.Err()
	zlog.Debug("shutting down relayer because source was shut down", zap.Error(err))
	r.Shutdown(err)
}

func (r *Relayer) sourcesBlockRefGetter() bstream.BlockRefGetter {
	var getters []bstream.BlockRefGetter
	for _, mindreaderAddr := range r.sourceAddresses {
		getters = append(getters, bstream.StreamHeadBlockRefGetter(mindreaderAddr))
	}
	return bstream.HighestBlockRefGetter(getters...)
}

func (r *Relayer) monitorBlockHole(triggerRestart func()) {
	for {
		time.Sleep(1 * time.Second)
		if r.blockHoleDetected() {
			triggerRestart()
			r.resetBlockHoleMonitoring()
			return
		}
	}
}
