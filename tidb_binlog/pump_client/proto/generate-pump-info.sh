#!/usr/bin/env bash
cd proto/

echo "generate pump info code..."
GOGO_ROOT=${GOPATH}/src/github.com/gogo/protobuf
protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --gofast_out=../go-pump-info pump_info.proto
cd ../go-pump-info
sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
sed -i.bak -E 's/import fmt \"fmt\"//g' *.pb.go
rm -f *.bak
goimports -w *.pb.go