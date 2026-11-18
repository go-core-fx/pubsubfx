package pubsubfx

import (
	"context"
	"fmt"

	"github.com/go-core-fx/fxutil"
	"github.com/go-core-fx/pubsubfx/pubsub"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// ModuleName is the fx module and logger name.
const ModuleName = "pubsubfx"

// Module provides the PubSub selected by Config and closes it when the app
// stops, so a Redis-backed instance releases its subscriptions and client
// even on graceful shutdown paths that never call Close themselves.
func Module() fx.Option {
	return fx.Module(
		ModuleName,
		fxutil.WithNamedLogger(ModuleName),
		fx.Provide(New),
		fx.Invoke(func(ps pubsub.PubSub, logger *zap.Logger, lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(_ context.Context) error {
					return nil
				},
				OnStop: func(_ context.Context) error {
					if err := ps.Close(); err != nil {
						logger.Error("pubsub close failed", zap.Error(err))

						return fmt.Errorf("failed to close pubsub: %w", err)
					}

					return nil
				},
			})
		}),
	)
}
