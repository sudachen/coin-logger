module github.com/sudachen/coin-logger

go 1.12

replace github.com/sudachen/coin-exchange => ./.deps/coin-exchange

require github.com/sudachen/coin-exchange v0.0.0

require (
	github.com/apache/thrift v0.12.0 // indirect
	github.com/aws/aws-sdk-go v1.20.20
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/logger v1.0.1
	github.com/stretchr/testify v1.3.0 // indirect
	github.com/xitongsys/parquet-go v1.3.0
	github.com/xitongsys/parquet-go-source v0.0.0-20190611011107-a9b8f78bccbe
	golang.org/x/net v0.0.0-20190628185345-da137c7871d7 // indirect
	gopkg.in/yaml.v3 v3.0.0-20190709130402-674ba3eaed22
)
