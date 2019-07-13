package internal

import (
	"fmt"
	"strings"
	"testing"
)

const Config_t1 = `
s3:
  host: ams3.digitaloceanspaces.com
  prefix: coin-work/history/
  key: S3KEY
  secret: S3SECRET

exchanges:
  - Binance

coins:
  btc: [usd]
  eth: [btc, usd]
  xrp: [btc, usd]
  ltc: [btc, usd]
  bch: [btc, usd]
`

func Test1(t *testing.T) {
	cfg := &Config{}
	if err := cfg.UnmarshalYAML([]byte(Config_t1)); err != nil {
		t.Errorf("%v", err)
		return
	}
	fmt.Printf("%#v\n", *cfg)
}

func Test2(t *testing.T) {
	fmt.Println(strings.FieldsFunc("a-b-c.d~", func(c rune) bool { return c == '-'}))
}