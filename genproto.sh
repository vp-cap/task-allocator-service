#!/bin/bash -e

set -e 

PROTODIR=../proto

mkdir -p genproto
protoc --go_out=plugins=grpc:genproto -I $PROTODIR $PROTODIR/task.proto
