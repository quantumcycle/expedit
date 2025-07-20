package subscriber

import (
	"context"
	"github.com/avast/retry-go/v4"
	"time"
)

// BackoffRetryOnAckNackError is a helper function to create an OnAckErrorFn that retries the
// AckNackFn on error. You need to provide a fallback method `onRetryFail` in case the
// retries fails
func BackoffRetryOnAckNackError[T any](
	retryAttempts uint,
	minDelay, maxDelay time.Duration,
	onRetryFail OnAckNackErrorFn[T]) OnAckNackErrorFn[T] {
	retryOptions := []retry.Option{
		retry.DelayType(retry.BackOffDelay),
		retry.Attempts(retryAttempts),
		retry.Delay(minDelay),
		retry.MaxDelay(maxDelay),
		retry.LastErrorOnly(true),
	}

	return func(ctx context.Context, msgImpl T, ack bool, ackFn AckNackFn[T], err error) {
		retryErr := retry.Do(
			func() error {
				return ackFn(ctx, msgImpl)
			},
			retryOptions...)
		if retryErr != nil {
			onRetryFail(ctx, msgImpl, ack, ackFn, retryErr)
		}
	}

}
