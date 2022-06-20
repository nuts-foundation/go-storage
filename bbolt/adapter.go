package bbolt

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/nuts-foundation/go-stoabs"
	"go.etcd.io/bbolt"
	"os"
)

var _ stoabs.Adapter = (*adapter)(nil)

const defaultFileMode = os.FileMode(0640)

func CreateAdapter() stoabs.Adapter {
	return &adapter{}
}

type adapter struct {
}

// CreateStore creates a new BBolt-backed KV store.
func (b adapter) CreateStore(config map[string]interface{}) (stoabs.KVStore, error) {
	configCopy := copyMap(config)

	// Set defaults
	flags := b.FlagSet()
	err := flags.Parse([]string{})
	if err != nil {
		return nil, err
	}
	flags.VisitAll(func(f *flag.Flag) {
		// If not set and default is defined, override
		if _, set := configCopy[f.Name]; set {
			return
		}
		if len(f.DefValue) == 0 {
			return
		}
		_ = f.Value.Set(f.DefValue)
		configCopy[f.Name] = f.Value
	})

	// Remarshal map into BBolt config struct
	data, _ := json.Marshal(configCopy)
	cfg := Config{}
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		return nil, fmt.Errorf("invalid BBolt database config: %w", err)
	}

	// Validate
	if len(cfg.Path) == 0 {
		return nil, fmt.Errorf("invalid BBolt database config: path not set")
	}

	// Collect BBolt options
	bboltOpts := *bbolt.DefaultOptions
	if cfg.NoSync {
		bboltOpts.NoSync = true
		bboltOpts.NoFreelistSync = true
		bboltOpts.NoGrowSync = true
	}
	return createBBoltStore(&bboltOpts, cfg)
}

func (b adapter) FlagSet() *flag.FlagSet {
	result := flag.NewFlagSet("bbolt", flag.ContinueOnError)
	_ = result.String("path", "", "File path to the BBolt database.")
	_ = result.Uint("filemode", uint(defaultFileMode), "POSIX file mode to use when creating the BBolt database.")
	return result
}

func copyMap(src map[string]interface{}) map[string]interface{} {
	data, _ := json.Marshal(src)
	result := make(map[string]interface{}, 0)
	_ = json.Unmarshal(data, &result)
	return result
}
