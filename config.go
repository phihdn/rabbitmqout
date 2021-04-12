package rabbitmqout

import (
	"fmt"

	"github.com/elastic/beats/v7/libbeat/common/file"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
)

type config struct {
	Path            string       `config:"path"`
	Filename        string       `config:"filename"`
	RotateEveryKb   uint         `config:"rotate_every_kb" validate:"min=1"`
	NumberOfFiles   uint         `config:"number_of_files"`
	Codec           codec.Config `config:"codec"`
	Permissions     uint32       `config:"permissions"`
	RotateOnStartup bool         `config:"rotate_on_startup"`
}

var (
	defaultConfig = config{
		NumberOfFiles:   7,
		RotateEveryKb:   10 * 1024,
		Permissions:     0600,
		RotateOnStartup: true,
	}
)

func (c *config) Validate() error {
	if c.NumberOfFiles < 2 || c.NumberOfFiles > file.MaxBackupsLimit {
		return fmt.Errorf("The number_of_files to keep should be between 2 and %v",
			file.MaxBackupsLimit)
	}

	return nil
}