package isink

import (
	"context"
	"fmt"
	"strings"

	"github.com/algonode/algostreamer/internal/config"
	log "github.com/sirupsen/logrus"
)

type SinkFactory interface {
	Name() string
	Build(ctx context.Context, cfg *config.SinkDef, log *log.Logger) (Sink, error)
}

var indexerFactories map[string]SinkFactory

func RegisterFactory(name string, factory SinkFactory) {
	if indexerFactories == nil {
		indexerFactories = make(map[string]SinkFactory)
	}
	fmt.Printf("New Factory '%s'\n", name)
	indexerFactories[strings.ToLower(name)] = factory
}

func SinkByName(ctx context.Context, name string, cfg *config.SinkDef, log *log.Logger) (Sink, error) {
	if val, ok := indexerFactories[strings.ToLower(name)]; ok {
		return val.Build(ctx, cfg, log)
	}
	for i, n := range indexerFactories {
		fmt.Printf("Factory %d = '%s'\n", i, n)
	}
	return nil, fmt.Errorf("no Sink factory for %s", name)
}
