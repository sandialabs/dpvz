/*******************************************************************************
 * Copyright (c) 2025, Sandia National Laboratories                            *
 * All rights reserved.                                                        *
 *                                                                             *
 * Redistribution and use in source and binary forms, with or without          *
 * modification, are permitted provided that the following conditions          *
 * are met:                                                                    *
 *                                                                             *
 *  o Redistributions of source code must retain the above copyright           *
 *    notice, this list of conditions and the following disclaimer.            *
 *  o Redistributions in binary form must reproduce the above copyright        *
 *    notice, this list of conditions and the following disclaimer listed      *
 *    in this license in the documentation and/or other materials provided     *
 *    with the distribution.                                                   *
 *  o Neither the name of the copyright holders nor the names of its           *
 *    contributors may be used to endorse or promote products derived          *
 *    from this software without specific prior written permission.            *
 *                                                                             *
 * The copyright holders provide no reassurances that the source code          *
 * provided does not infringe any patent, copyright, or any other              *
 * intellectual property rights of third parties. The copyright holders        *
 * disclaim any liability to any recipient for claims brought against          *
 * recipient by any third party for infringement of that parties intellectual  *
 * property rights.                                                            *
 *                                                                             *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" *
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE   *
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE  *
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE   *
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR         *
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF        *
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS    *
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN     *
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)     *
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF      *
 * THE POSSIBILITY OF SUCH DAMAGE.                                             *
 *******************************************************************************/

#if !defined(DPVZVTK_H)
#define DPVZVTK_H

#include <float.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>


#include <string>

#include "DPvzFile.h"

#if defined(DPVZ_MPI)
#include <mpi.h>
#endif

// #include "DPvzMetadata.h"

#define DPVTK_GLOBAL_SIZE	(((DPVZ_PAGE_SIZE-sizeof(DPvzMetadata))/1024)*1024)
#define DPVTK_MAJIK		"DPvzVtk"
#define DPVTK_EXT		"dpvtk"

class DPvzVtk : public DPvzFile {
public:

#if defined(DPVZ_MPI)
  // parallel (MPI) constructor, intended to read and/or write files
  // it may also be used by distributed tools to read files
  DPvzVtk(std::string name, DPvzMode mode, MPI_Comm c=MPI_COMM_WORLD, bool repair=false);

#else
  // serial (non-MPI) constructor, intended for use by serial or OpenMP-threaded tools to read files
  // it may also be used by a serial code to write a file (write assumes # of ranks == 1, rank id == 0)
  DPvzVtk(std::string name, DPvzMode mode, bool repair=false);

#endif

  ~DPvzVtk();

  static int list(FILE* fp, const char* file, const char* path=".", int64_t cycle_start=LLONG_MIN, int64_t cycle_end=LLONG_MAX, int64_t cycle_stride=1, double time_start=-DBL_MAX, double time_end=DBL_MAX);
  static int show(FILE* fp, const char* file, const char* path=".", int64_t cycle_start=LLONG_MIN, int64_t cycle_end=LLONG_MAX, int64_t cycle_stride=1, double time_start=-DBL_MAX, double time_end=DBL_MAX, bool crc=true);
  static int extract(const char* file, const char* path=".", int64_t cycle_start=LLONG_MIN, int64_t cycle_end=LLONG_MAX, int64_t cycle_stride=1, double time_start=-DBL_MAX, double time_end=DBL_MAX);
  static int main(int argc, char**argv);

};

#endif
