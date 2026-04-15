#!/bin/bash

pushd ../src
./build.sh
popd

arch=cts2
arch=cts1


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
CWD=$PWD
UP1=`dirname $CWD`
UP2=`dirname $UP1`
UP3=`dirname $UP2`
UP4=`dirname $UP3`
UP5=`dirname $UP4`
moddir=$UP5/automate/modules/$arch

echo '$' module use $moddir
module use $moddir

SRC=( dpvtk-ar.C )

INCDIR=$UP1/src


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
$CC $CFLAGS ${SRC[*]} -I $INCDIR -o $exe -lz -L $LIBDIR -l $LIB

CFLAGS="$CFMPI"
CC="$CCMPI"
LIB=DPvzMpi
exe=dpvtk-ar-mpi
$CC $CFLAGS ${SRC[*]} -I $INCDIR -o $exe -lz -L $LIBDIR -l $LIB
