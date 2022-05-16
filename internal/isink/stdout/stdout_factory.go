package stdout

import (
	"context"

	"github.com/algonode/algostreamer/internal/config"
	"github.com/algonode/algostreamer/internal/isink"
	log "github.com/sirupsen/logrus"
)

type stdoutFactory struct {
}

func (df stdoutFactory) Name() string {
	return "stdout"
}

func (df stdoutFactory) Build(ctx context.Context, cfg *config.SinkDef, log *log.Logger) (isink.Sink, error) {
	return Make(ctx, cfg, log)
}

func init() {
	isink.RegisterFactory("stdout", &stdoutFactory{})
}
