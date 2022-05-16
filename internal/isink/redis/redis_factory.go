package redis

import (
	"context"

	"github.com/algonode/algostreamer/internal/config"
	"github.com/algonode/algostreamer/internal/isink"
	log "github.com/sirupsen/logrus"
)

const Redis = "redis"

type redisFactory struct {
}

func (df redisFactory) Name() string {
	return Redis
}

func (df redisFactory) Build(ctx context.Context, cfg *config.SinkDef, log *log.Logger) (isink.Sink, error) {
	return Make(ctx, cfg, log)
}

func init() {
	isink.RegisterFactory(Redis, &redisFactory{})
}
