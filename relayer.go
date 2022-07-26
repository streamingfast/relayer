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
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/blockstream"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/dgrpc"
	"github.com/streamingfast/dstore"
	pbheadinfo "github.com/streamingfast/pbgo/sf/headinfo/v1"
	pbmerger "github.com/streamingfast/pbgo/sf/merger/v1"
	"github.com/streamingfast/relayer/metrics"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	pbhealth "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	getHeadInfoTimeout = 10 * time.Second
)

var traceBlockSource = os.Getenv("TRACE_RELAYER_SOURCE") == "true"

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

	sourceConnCache       map[string]*grpc.ClientConn
	sourceHeadClientCache map[string]pbheadinfo.HeadInfoClient
	joiningSource         *bstream.JoiningSource
}

func NewRelayer(blockFilter func(blk *bstream.Block) error, sourceAddresses []string, mergerAddr string, maxSourceLatency time.Duration, grpcListenAddr string, bufferSize, sourceRequestBurst int) *Relayer {
	r := &Relayer{
		Shutter:               shutter.New(),
		blockFilter:           blockFilter,
		sourceAddresses:       sourceAddresses,
		mergerAddr:            mergerAddr,
		maxSourceLatency:      maxSourceLatency,
		grpcListenAddr:        grpcListenAddr,
		sourceRequestBurst:    int64(sourceRequestBurst),
		highestSentBlockRef:   bstream.BlockRefEmpty,
		sourceConnCache:       make(map[string]*grpc.ClientConn),
		sourceHeadClientCache: make(map[string]pbheadinfo.HeadInfoClient),
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
	repeatedLogger := newRepeatingLogger(zlog, repeatingLoggerConfig{RepeatLevel: zap.DebugLevel, RepeatEach: 30 * time.Second, ResetToFirstEach: 3 * time.Minute})

	for {
		select {
		case <-time.After(sleepTime):
			sleepTime = 500 * time.Millisecond
		case <-r.Terminating():
			return
		}

		headInfo := r.fetchHighestHeadInfo(repeatedLogger)
		if headInfo == nil {
			repeatedLogger.Log("cannot get head info mindreader, retrying forever", zap.Any("sources_addr", r.sourceAddresses))
			continue
		}

		headTime := headInfo.HeadTime.AsTime()

		r.blockStreamServer.SetHeadInfo(headInfo.HeadNum, headInfo.HeadID, headTime, headInfo.LibNum)
		r.ready = true

		observedLatency := time.Since(headTime)
		if observedLatency < maxSourceLatency {
			readyStartBlock <- calculateDesiredStartBlock(headInfo.HeadNum, headInfo.LibNum, minOffsetToHead)
			return
		} else {
			repeatedLogger.Log("source head latency too high", zap.Any("sources_addr", r.sourceAddresses), zap.Duration("max_source_latency", maxSourceLatency), zap.Duration("observed_latency", observedLatency))
		}
	}
}

type repeatingLoggerConfig struct {
	FirstLevel       zapcore.Level
	RepeatLevel      zapcore.Level
	RepeatEach       time.Duration
	ResetToFirstEach time.Duration
}

type repeatingLogger struct {
	startTime        time.Time
	lastRepeatedTime time.Time
	firstPassed      bool

	logger *zap.Logger
	config repeatingLoggerConfig
}

func newRepeatingLogger(logger *zap.Logger, config repeatingLoggerConfig) *repeatingLogger {
	return &repeatingLogger{
		startTime:        time.Now(),
		lastRepeatedTime: time.Now(),

		logger: logger,
		config: config,
	}
}

func (l *repeatingLogger) Log(msg string, fields ...zapcore.Field) {
	if l.firstPassed && l.config.ResetToFirstEach != 0 && time.Since(l.startTime) > l.config.ResetToFirstEach {
		l.startTime = time.Now()
		l.lastRepeatedTime = time.Now()
		l.firstPassed = false
	}

	level := l.config.RepeatLevel
	if !l.firstPassed {
		level = l.config.FirstLevel
	}

	shouldRepeat := true
	if l.config.RepeatEach != 0 {
		if time.Since(l.lastRepeatedTime) > l.config.RepeatEach {
			l.lastRepeatedTime = time.Now()
		} else {
			shouldRepeat = false
		}
	}

	if !l.firstPassed || shouldRepeat {
		l.logger.Check(level, msg).Write(fields...)
	}

	l.firstPassed = true
}

func (r *Relayer) fetchHighestHeadInfo(logger *repeatingLogger) (headInfo *pbheadinfo.HeadInfoResponse) {
	for _, addr := range r.sourceAddresses {
		headInfoClient := r.sourceHeadClientCache[addr]
		if headInfoClient == nil {
			conn, err := dgrpc.NewInternalClient(addr)
			if err != nil {
				zlog.Info("cannot connect to backend", zap.String("address", addr))
				continue
			}

			headInfoClient = pbheadinfo.NewHeadInfoClient(conn)
			r.sourceHeadClientCache[addr] = headInfoClient
			r.sourceConnCache[addr] = conn
		}

		ctx, cancel := context.WithTimeout(context.Background(), getHeadInfoTimeout)
		defer cancel()
		remoteHead, err := headInfoClient.GetHeadInfo(ctx, &pbheadinfo.HeadInfoRequest{}, grpc.WaitForReady(false))
		if err != nil || remoteHead == nil {
			logger.Log("cannot get headinfo from backend", zap.String("address", addr), zap.Error(err))
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

	detected := r.highestSentBlockRef.Num() != 0 && r.highestReceivedBlockNum > (r.highestSentBlockRef.Num()+1)

	if detected {
		zlog.Error("Found a hole in block seq [r.highestReceivedBlockNum > (r.highestSentBlockRef.Num()+1)]", zap.Uint64("highest_receive_block", r.highestReceivedBlockNum), zap.Uint64("highest_sent_block", r.highestSentBlockRef.Num()))
	}

	return detected
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

		sourceName := urlToLoggerName(u)
		logger := zlog.Named("src").Named(sourceName)
		sf := func(subHandler bstream.Handler) bstream.Source {

			gate := bstream.NewRealtimeGate(r.maxSourceLatency, subHandler, bstream.GateOptionWithLogger(logger))
			var upstreamHandler bstream.Handler
			upstreamHandler = bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
				return gate.ProcessBlock(blk, &namedObj{
					Obj:  obj,
					Name: sourceName,
				})
			})

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
	return strings.TrimPrefix(strings.TrimPrefix(url, "dns:///"), ":")
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

	pipe := bstream.HandlerFunc(func(blk *bstream.Block, _ interface{}) error {
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
		forkable.WithFilters(bstream.StepNew),
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
			bstream.JoiningSourceTargetBlockNum(bstream.GetProtocolFirstStreamableBlock),
			bstream.JoiningSourceLiveTracker(200, r.sourcesBlockRefGetter()),
		}

		if startBlockRef != bstream.BlockRefEmpty {
			startBlock = startBlockRef.Num()
			jsOptions = append(jsOptions, bstream.JoiningSourceTargetBlockID(startBlockRef.ID()))
		}

		gate := bstream.NewBlockNumGate(startBlock, bstream.GateInclusive, h, bstream.GateOptionWithLogger(zlog))

		fileSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
			options := []bstream.FileSourceOption{
				bstream.FileSourceWithNotFoundCallBack(r.FileSourceNotFoundCallBack()),
			}
			return bstream.NewFileSource(blockStore, startBlock, 2, filterPreprocessFunc, subHandler, options...)
		})

		zlog.Info("new joining source with", zap.Uint64("start_block_num", startBlock))
		js := bstream.NewJoiningSource(fileSourceFactory, r.newMultiplexedSource, gate, jsOptions...)
		r.joiningSource = js

		go r.monitorBlockHole(func() { js.Shutdown(fmt.Errorf("hole detected in blocks")) }) // triggers eternalsource restart
		return js
	})

	h := bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
		sourceName := ""
		if named, ok := obj.(*namedObj); ok {
			obj = named.Obj
			sourceName = named.Name
		}

		r.lastBlockMutex.Lock()
		if blk.Number > r.highestReceivedBlockNum {
			r.highestReceivedBlockNum = blk.Number
			if traceBlockSource && sourceName != "" {
				zlog.Info("received block", zap.String("source_name", sourceName), zap.Uint64("block_number", blk.Number))
			}
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

	r.OnTerminated(func(_ error) {
		zlog.Info("closing head info client(s)", zap.Int("conn_count", len(r.sourceConnCache)))
		for _, conn := range r.sourceConnCache {
			conn.Close()
		}
	})

	r.source.Run()

	err := r.source.Err()
	zlog.Debug("shutting down relayer because source was shut down", zap.Error(err))
	r.Shutdown(err)
}

func (r *Relayer) FileSourceNotFoundCallBack() bstream.NotFoundCallbackFunc {
	if r.mergerAddr != "" {

		return func(nextBaseBlockNum uint64, highestFileProcessedBlock bstream.BlockRef, handler bstream.Handler, logger *zap.Logger) error {
			targetJoinBlock := lowestIDInBufferGTE(nextBaseBlockNum, r.joiningSource.BufferRef())
			if targetJoinBlock == nil {
				return nil
			}

			err := handleBlockFromMerger(
				logger,
				nextBaseBlockNum,
				targetJoinBlock.ID(),
				r.mergerAddr,
				handler,
			)

			if err != nil {
				logger.Info("cannot join using merger source", zap.Error(err), zap.Stringer("target_join_block", targetJoinBlock))
				return nil
			}

			logger.Info("launching source from merger", zap.Uint64("low_block_num", nextBaseBlockNum), zap.Stringer("target_join_block", targetJoinBlock))
			return nil
		}
	}
	return nil
}

func handleBlockFromMerger(logger *zap.Logger, blockNum uint64, blockID string, mergerAddr string, handler bstream.Handler) error {
	conn, err := dgrpc.NewInternalClient(mergerAddr)
	if err != nil {
		return fmt.Errorf("new connection to merger: %w", err)
	}

	client := pbmerger.NewMergerClient(conn)
	stream, err := client.PreMergedBlocks(
		context.Background(),
		&pbmerger.Request{
			LowBlockNum: blockNum,
			HighBlockID: blockID,
		},
		grpc.WaitForReady(false),
	)
	if err != nil {
		return fmt.Errorf("getting block stream from merger: %w", err)
	}

	header, err := stream.Header()
	if err != nil {
		return err
	}
	// we return failure to obtain blocks inside GRPC header
	if errmsgs := header.Get("error"); len(errmsgs) > 0 {
		return fmt.Errorf("%s", errmsgs[0])
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("receiving message from merger pre merger block stream: %w", err)
		}

		logger.Debug("receive pre merge block", zap.Uint64("block_num", resp.Block.Number), zap.String("block_id", resp.Block.Id))
		nativeBlock, err := bstream.NewBlockFromProto(resp.Block)
		if err != nil {
			return fmt.Errorf("block from proto: %w", err)
		}
		err = handler.ProcessBlock(nativeBlock, nil)
		if err != nil {
			return fmt.Errorf("handler process block: %w", err)
		}
	}
	return nil
}

func lowestIDInBufferGTE(blockNum uint64, buf *bstream.Buffer) (blk bstream.BlockRef) {
	for _, blk := range buf.AllBlocks() {
		if blk.Num() < blockNum {
			continue
		}
		return blk
	}
	return nil
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

type namedObj struct {
	Name string
	Obj  interface{}
}
