# Copyright (c) 2024, Sandia National Laboratories
# All rights reserved.

cmake_minimum_required(VERSION 3.12)

# set the project name
project(DP VERSION 2.7)

# turn on CTest
enable_testing()

# sane directory naming
include(GNUInstallDirs)

# create compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(CheckFunctionExists)
check_function_exists(memrchr, HAVE_MEMRCHR)

# set default toggles
option(DPVZ_MPI ON)
option(DPVZ_SERIAL OFF)
option(DPVZ_TEST_MPI ON)
option(DPVZ_TEST_SERIAL ON)

# use C++-11
# set(CMAKE_CXX_STANDARD 11)

# since testing prefers OpenMPI and shared-memory
# set(MPIEXEC_SETUPFLAGS --mca btl self,vader --mca orte_allowed_exit_without_sync 1)

# add projects
add_subdirectory(src)
if(DPVZ_SERIAL AND DPVZ_TEST_SERIAL)
  add_subdirectory(tests/ser)
endif(DPVZ_SERIAL AND DPVZ_TEST_SERIAL)
if(DPVZ_MPI AND DPVZ_TEST_MPI)
  add_subdirectory(tests/mpi)
endif(DPVZ_MPI AND DPVZ_TEST_MPI)

# use CPack for creating packages
get_filename_component(PACK_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}"
                       REALPATH BASE_DIR "${CMAKE_BINARY_DIR}")
set(CPACK_GENERATOR TGZ)
set(CPACK_PACKAGE_DIRECTORY ${PACK_INSTALL_DIR}/../_package)
include(CPack)
