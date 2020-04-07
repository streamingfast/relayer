package relayer

import (
	"fmt"
	"net"

	"go.uber.org/zap"
)

func (r *Relayer) StartListening(bufferSize int) error {
	lis, err := net.Listen("tcp", r.grpcListenAddr)
	if err != nil {
		return fmt.Errorf("failed listening grpc %q: %w", r.grpcListenAddr, err)
	}
	zlog.Info("tcp listener created")

	go func() {
		zlog.Info("listening & serving grpc content", zap.String("grpc_listen_addr", r.grpcListenAddr))
		if err := r.blockStreamServer.Serve(lis); err != nil {
			r.Shutdown(fmt.Errorf("error on grpc serve: %w", err))
			return
		}
	}()
	return nil
}
