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

package main

import (
	"fmt"
	_ "net/http/pprof"
	"time"

	"github.com/abourget/viperbind"
	"github.com/dfuse-io/derr"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	relayerapp "github.com/dfuse-io/relayer/app/relayer"
	"github.com/dfuse-io/relayer/metrics"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var rootCmd = &cobra.Command{Use: "relayer", Short: "Operate the relayer", RunE: relayerRunE}

func main() {
	cobra.OnInitialize(func() {
		viperbind.AutoBind(rootCmd, "RELAYER")
	})

	rootCmd.PersistentFlags().String("grpc-listen-addr", ":9000", "Listening address for gRPC service serving blocks")
	rootCmd.PersistentFlags().StringSlice("source", []string{":9001"}, "List of blockstream sources to connect to for live block feeds (repeat flag as needed)")
	rootCmd.PersistentFlags().String("merger-addr", ":9001", "Address for grpc merger service")
	rootCmd.PersistentFlags().Int("buffer-size", 50, "number of blocks that will be kept and sent immediately on connection")
	rootCmd.PersistentFlags().Duration("max-drift", 300*time.Second, "max delay between live blocks before we die in hope of a better world")
	rootCmd.PersistentFlags().Uint64("min-start-offset", 200, "number of blocks before HEAD where we want to start for faster buffer filling (missing blocks come from files/merger)")
	rootCmd.PersistentFlags().Duration("max-source-latency", 300*time.Second, "max latency tolerated to connect to a source")
	rootCmd.PersistentFlags().Duration("init-time", 150*time.Second, "time before we start looking for max drift")
	rootCmd.PersistentFlags().String("source-store", "gs://example/blocks", "Store path url to read batch files from")
	rootCmd.PersistentFlags().String("protocol", "ETH", "Protocol in string, must fit with bstream.ProtocolRegistry")

	derr.Check("running relayer", rootCmd.Execute())

}

func relayerRunE(cmd *cobra.Command, args []string) (err error) {
	setup()

	protocolString := viper.GetString("global-protocol")
	protocol := pbbstream.Protocol(pbbstream.Protocol_value[protocolString])
	if protocol == pbbstream.Protocol_UNKNOWN {
		derr.Check("invalid protocol", fmt.Errorf("protocol value: %q", protocolString))
	}
	// TODO: test whether we have an _underscore-imported_ implementation for that protocolString

	go metrics.ServeMetrics()

	config := &relayerapp.Config{
		SourcesAddr:      viper.GetStringSlice("global-source"),
		GRPCListenAddr:   viper.GetString("global-grpc-listen-addr"),
		MergerAddr:       viper.GetString("global-merger-addr"),
		BufferSize:       viper.GetInt("global-buffer-size"),
		MaxDrift:         viper.GetDuration("global-max-drift"),
		MinStartOffset:   viper.GetUint64("global-min-start-offset"),
		MaxSourceLatency: viper.GetDuration("global-max-source-latency"),
		InitTime:         viper.GetDuration("global-init-time"),
		SourceStoreURL:   viper.GetString("global-source-store"),
		Protocol:         protocol,
	}
	app := relayerapp.New(config)
	derr.Check("relayer app run", app.Run())

	select {
	case <-app.Terminated():
		zlog.Info("relayer is done", zap.Error(app.Err()))
	case sig := <-derr.SetupSignalHandler(viper.GetDuration("global-shutdown-drain-delay")):
		zlog.Info("terminating through system signal", zap.Reflect("sig", sig))
		app.Shutdown(nil)
	}

	return
}
