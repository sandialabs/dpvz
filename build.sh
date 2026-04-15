#!/bin/bash

export TRACE="fprintf"
export TRACE="// "

pushd src
./build.sh
popd

pushd tests/mpi
./build.sh
popd
