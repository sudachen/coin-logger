package internal

import (
	"fmt"
	"github.com/sudachen/coin-exchange/exchange"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"path"
	//"path"
)

type S3 struct {
	Host   string `yaml:"host"`
	Prefix string `yaml:"prefix"`
	Key    string `yaml:"key"`
	Secret string `yaml:"secret"`
	Cache  string `yaml:"cache"`
	Hours  int32  `yaml:"hours"`
}

type Config struct {
	S3        S3                  `yaml:"s3"`
	Pairs     []exchange.CoinPair `yaml:"-"`
	Exchanges []exchange.Exchange `yaml:"exchanges"`
}

func (cfg *Config) UnmarshalYAML(bs []byte) error {
	type Alias Config
	aux := &struct {
		Coins  map[exchange.CoinType][]exchange.CoinType `yaml:"coins"`
		*Alias `yaml:",inline"`
	}{Alias: (*Alias)(cfg)}
	if err := yaml.Unmarshal(bs, &aux); err != nil {
		return err
	}
	for k, v := range aux.Coins {
		for _, c := range v {
			cfg.Pairs = append(cfg.Pairs, exchange.CoinPair{k, c})
		}
	}
	return nil
}

func findConfig(s string) *string {
	if d, _ := path.Split(s); d != "" {
		return &s
	}

	if path.Ext(s) == "" {
		s += ".yml"
	}

	s1 := path.Join(".", s)
	if _, err := os.Stat(s1); err == nil {
		return &s1
	}

	infra := path.Join(".", "infra", s)
	if _, err := os.Stat(infra); err == nil {
		return &infra
	}

	if home, ok := os.LookupEnv("HOME"); ok {
		s2 := path.Join(home, ".infra", s)
		if _, err := os.Stat(s2); err == nil {
			return &s2
		}
	}
	s3 := path.Join("etc", "infra", s)
	if _, err := os.Stat(s3); err == nil {
		return &s3
	}
	return nil
}

func LoadConfig(name string) (*Config, error) {
	if name == "" {
		name = "coin-logger"
	}
	if cfgPath := findConfig(name); cfgPath != nil {
		if bs, err := ioutil.ReadFile(*cfgPath); err != nil {
			return nil, err
		} else {
			cfg := &Config{}
			if err := cfg.UnmarshalYAML(bs); err != nil {
				return nil, err
			}
			return cfg, nil
		}
	}
	return nil, fmt.Errorf("can't find config '%v'", name)
}
