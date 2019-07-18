package internal

import (
	"fmt"
	"testing"
	"time"
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
	x, e := time.Parse("2006-01-02T15:04:05.000Z", "2019-07-18T03:54:00.000Z")
	fmt.Println(x, e)
	layout := "2014-09-12T11:45:26.371Z"
	str := "2014-11-12T11:45:26.371Z"
	y, e := time.Parse(layout, str)
	fmt.Println(y, e)
}
