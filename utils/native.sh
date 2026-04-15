#!/bin/bash

pushd ../src
./native.sh
popd

TRACE="fprintf"
TRACE="//"
CFSER="-DDPVZ_SERIAL -Wall -O3"
CFMPI="-DDPVZ_MPI -Wall -O3"
CFOMP="-DDPVZ_OMP -Wall -O3 -fopenmp"
CCSER=g++
CCOMP=g++
CCMPI=mpiCC

mode=SERIAL

for i in $* ; do
    if [[ $i == *=* ]] ; then
	export $i
    fi
done

SRC=( dpvtk-ar.C )

if [[ $INCDIR = "" || ! -d $INCDIR ]] ; then
    INCDIR=../src
fi


if [[ $LIBDIR = "" ]] ; then
    LIBDIR=../lib64
    mkdir -p $LIBDIR
fi
export LD_LIBRARY_PATH=$LIBDIR:$LD_LIBRARY_PATH


set -x

CFLAGS="$CFSER"
CC="$CCSER"
LIB=DPvzSer
exe=dpvtk-ar-ser
echo $CC $CFLAGS ${SRC[*]} -I $INCDIR -o $exe -lz -L $LIBDIR -l $LIB
$CC $CFLAGS ${SRC[*]} -I $INCDIR -o $exe -lz -L $LIBDIR -l $LIB

CFLAGS="$CFMPI"
CC="$CCMPI"
LIB=DPvzMpi
exe=dpvtk-ar-mpi
echo $CC $CFLAGS ${SRC[*]} -I $INCDIR -o $exe -lz -L $LIBDIR -l $LIB
$CC $CFLAGS ${SRC[*]} -I $INCDIR -o $exe -lz -L $LIBDIR -l $LIB
