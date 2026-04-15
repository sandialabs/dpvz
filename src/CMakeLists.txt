# Copyright (c) 2024, Sandia National Laboratories
# All rights reserved.

cmake_minimum_required(VERSION 3.12)
cmake_policy(SET CMP0074 NEW)

find_package(MPI REQUIRED)
find_package(ZLIB REQUIRED)

# define source and header lists for easy reuse
set(dpvz_src     DPvzErr.C  DPvzFile.C  DPvzGlobal.C  DPvzMetadata.C  DPvzMode.C  DPvzRankToc.C  DPvzToc.C  DPvzTocEntry.C  DPvzTocIndex.C  DPvzUtil.C  DPvzVtk.C DPvzVtkData.C)
set(dpvz_header  DPvzErr.h  DPvzFile.h  DPvzGlobal.h  DPvzMetadata.h  DPvzMode.h  DPvzRankToc.h  DPvzTocEntry.h  DPvzToc.h  DPvzTocIndex.h  DPvzUtil.h  DPvzVtk.h DPvzVtkData.h)

set(additional_defs "")
if (HAVE_MEMRCHR)
  list(APPEND additional_defs "HAVE_MEMRCHR")
endif()

# define MPI library
if(DPVZ_MPI)
  add_library(DPvzMpi STATIC ${dpvz_src})
  add_library(DPvzMpi::DPvzMpi ALIAS DPvzMpi)
  set_target_properties(DPvzMpi PROPERTIES PUBLIC_HEADER "${dpvz_header}" POSITION_INDEPENDENT_CODE ON)
  target_compile_options(DPvzMpi PRIVATE -DDPVZ_MPI )
  target_compile_definitions(DPvzMpi PRIVATE "${additional_defs}")
  target_link_libraries(DPvzMpi PUBLIC ZLIB::ZLIB)
  target_link_libraries(DPvzMpi PUBLIC MPI::MPI_CXX)
  target_include_directories(DPvzMpi PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
  install(TARGETS DPvzMpi EXPORT DPvzMpi_targets INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  export(EXPORT DPvzMpi_targets NAMESPACE DPvzMpi:: FILE "${CMAKE_INSTALL_LIBDIR}/cmake/DPvzMpi/DPvzMpi-config.cmake")
if(DDPvzTrace)
  target_compile_options(DPvzMpi PRIVATE  -DDPvzTrace=ON)
endif(DDPvzTrace)
endif(DPVZ_MPI)

# define serial library
if(DPVZ_SERIAL)
  add_library(DPvzSer STATIC ${dpvz_src})
  add_library(DPvzSer::DPvzSer ALIAS DPvzSer)
  set_target_properties(DPvzSer PROPERTIES PUBLIC_HEADER "${dpvz_header}" POSITION_INDEPENDENT_CODE ON)
  target_compile_options(DPvzSer PRIVATE -DDPVZ_SERIAL )
  target_compile_definitions(DPvzSer PRIVATE "${additional_defs}")
  target_link_libraries(DPvzMpi PUBLIC ZLIB::ZLIB)
  target_include_directories(DPvzSer PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
  install(TARGETS DPvzSer EXPORT DPvzSer_targets INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  export(EXPORT DPvzSer_targets NAMESPACE DPvzSer:: FILE "${CMAKE_INSTALL_LIBDIR}/cmake/DPvzSer/DPvzSer-config.cmake")
if(DDPvzTrace)
  target_compile_options(DPvzSer PRIVATE  -DDPvzTrace=ON)
endif(DDPvzTrace)
endif(DPVZ_SERIAL)

