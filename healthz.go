package relayer

import (
	"context"

	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
)

func (r *Relayer) Check(ctx context.Context, in *pbhealth.HealthCheckRequest) (*pbhealth.HealthCheckResponse, error) {
	status := pbhealth.HealthCheckResponse_NOT_SERVING
	if r.ready {
		status = pbhealth.HealthCheckResponse_SERVING
	}
	return &pbhealth.HealthCheckResponse{
		Status: status,
	}, nil
}
