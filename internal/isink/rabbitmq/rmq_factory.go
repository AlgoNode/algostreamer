package rabbitmq

import (
	"context"

	"github.com/algonode/algostreamer/internal/config"
	"github.com/algonode/algostreamer/internal/isink"
	log "github.com/sirupsen/logrus"
)

const RabbitMQ = "rabbitmq"

type rmqFactory struct {
}

func (df rmqFactory) Name() string {
	return RabbitMQ
}

func (df rmqFactory) Build(ctx context.Context, cfg *config.SinkDef, log *log.Logger) (isink.Sink, error) {
	return Make(ctx, cfg, log)
}

func init() {
	isink.RegisterFactory(RabbitMQ, &rmqFactory{})
}
