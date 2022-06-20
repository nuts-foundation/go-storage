package bbolt

import "github.com/nuts-foundation/go-stoabs"

type Config struct {
	stoabs.Config
	Path     string `json:"path"`
	FileMode uint   `json:"filemode"`
}
