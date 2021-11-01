#!/usr/bin/env bash
cd proto/

echo "generate binlog code..."
GOGO_ROOT=${GOPATH}/src/github.com/gogo/protobuf
protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --gofast_out=../go-binlog secondary_binlog.proto
cd ../go-binlog
sed -i.bak -E 's/_ \"github.com\/gogo\/protobuf\/gogoproto\"//g' *.pb.go
sed -i.bak -E 's/import fmt \"fmt\"//g' *.pb.go
rm -f *.bak
goimports -w *.pb.go
