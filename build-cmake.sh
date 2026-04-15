#!/bin/sh
# -*- compile-command: "module load aue/gcc/10.3.0 ; module load aue/openmpi/4.1.6-gcc-10.3.0 ; module load aue/cmake ; ./build-cmake.sh" -*-

dir_src="`pwd -P`"
dir_build="`pwd -P`/../build-cmake"
dir_install="`pwd -P`/../install-cmake"

BUILD_COMPILER_C="${BUILD_COMPILER_C:-$( which gcc )}"
BUILD_COMPILER_CXX="${BUILD_COMPILER_CXX:-$( which g++ )}"
MPI_COMPILER_C="${MPI_COMPILER_C:-$( which mpicc )}"
MPI_COMPILER_CXX="${MPI_COMPILER_CXX:-$( which mpicxx )}"
MPIEXEC_PATH="${MPIEXEC_PATH:-$( which mpiexec )}"
SIMULTANEOUS=${SIMULTANEOUS:-32}

if test -d "${dir_install}" ; then
    rm -rf "${dir_install}"
fi
if test -d "${dir_build}" ; then
    rm -rf "${dir_build}"
fi
mkdir -p "${dir_build}"
pushd "${dir_build}"

time cmake \
    -D CMAKE_C_COMPILER=${BUILD_COMPILER_C} \
    -D CMAKE_CXX_COMPILER=${BUILD_COMPILER_CXX} \
    -D MPI_C_COMPILER=${MPI_COMPILER_C} \
    -D MPI_CXX_COMPILER=${MPI_COMPILER_CXX} \
    -D MPIEXEC_EXECUTABLE:PATH="${MPIEXEC_PATH}" \
    -D CMAKE_RELEASE_TYPE:STRING="Release" \
    -D DPVZ_MPI:BOOL="ON" \
    -D DPVZ_SERIAL:BOOL="ON" \
    -D DPVZ_TEST_MPI:BOOL="ON" \
    -D DPVZ_TEST_SERIAL:BOOL="ON" \
    -D CMAKE_INSTALL_PREFIX:PATH="${dir_install}" \
    -D ZLIB_ROOT:PATH="/usr/lib64" \
    "${dir_src}"
make VERBOSE=1 -j $SIMULTANEOUS
make install
ctest -j $SIMULTANEOUS

popd

exit 0
