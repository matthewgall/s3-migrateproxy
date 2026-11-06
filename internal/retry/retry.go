package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/smithy-go"
)

// Config controls retry behaviour.
type Config struct {
	MaxAttempts  int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	Multiplier float64
	Jitter     float64
}

// Default returns a sensible default retry config.
func Default() Config {
	return Config{
		MaxAttempts:  5,
		BaseDelay:  500 * time.Millisecond,
		MaxDelay:   30 * time.Second,
		Multiplier: 2.0,
		Jitter:     0.2,
	}
}

// Do executes fn until it succeeds, the context is cancelled, or retries are exhausted.
func Do(ctx context.Context, cfg Config, fn func() error) error {
	var lastErr error
	for i := 0; i < cfg.MaxAttempts; i++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isRetryable(lastErr) {
			return lastErr
		}
		if i == cfg.MaxAttempts-1 {
			break
		}
		delay := backoff(cfg, i)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("exhausted %d retries: %w", cfg.MaxAttempts, lastErr)
}

func backoff(cfg Config, attempt int) time.Duration {
	d := float64(cfg.BaseDelay)
	for i := 0; i < attempt; i++ {
		d *= cfg.Multiplier
	}
	if d > float64(cfg.MaxDelay) {
		d = float64(cfg.MaxDelay)
	}
	j := 1.0 + (rand.Float64()*2-1.0)*cfg.Jitter
	if j < 0 {
		j = 0
	}
	return time.Duration(d * j)
}

// IsNotFound returns true for authoritative object-not-found errors.
func IsNotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "NoSuchKey" || code == "NotFound" || code == "404"
	}
	return false
}

// Value retries a function that returns a value and an error.
func Value[T any](ctx context.Context, cfg Config, fn func() (T, error)) (T, error) {
	var zero T
	var v T
	err := Do(ctx, cfg, func() (err error) {
		v, err = fn()
		return err
	})
	if err != nil {
		return zero, err
	}
	return v, nil
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	// Don't retry not-found errors; they are terminal for the requested object.
	if IsNotFound(err) {
		return false
	}
	return true
}
