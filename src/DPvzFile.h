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

#if !defined(DPVZFILE_H)
#define DPVZFILE_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <string>

#if defined(DPVZ_SER) 
// DPVZ_SER is selected, change it to DPVZ_SERIAL, undefine DPVZ_SER and DPVZ_MPI
#define DPVZ_SERIAL
#undef DPVZ_SER
#undef DPVZ_MPI
#elif defined(DPVZ_SERIAL) 
// DPVZ_SERIAL is selected, undefine DPVZ_SER and DPVZ_MPI
#undef DPVZ_SER
#undef DPVZ_MPI
#elif defined(DPVZ_MPI) 
// DPVZ_MPI is selected, undefine DPVZ_SER and DPVZ_SERIAL
#undef DPVZ_SER
#undef DPVZ_SERIAL
#else
// nothing is selected, define DPVZ_MPI
#define DPVZ_MPI
#endif


#if defined(DPVZ_MPI)
#include <mpi.h>
#endif

#include "DPvzErr.h"
#include "DPvzMode.h"

#include "DPvzGlobal.h"
#include "DPvzMetadata.h"
#include "DPvzRankToc.h"
#include "DPvzToc.h"
#include "DPvzTocEntry.h"

static const int DPVZ_DEFAULT_STRIPE_SIZE  = (1<<20);
static const int DPVZ_DEFAULT_STRIPE_INDEX = (-1);
static const int DPVZ_DEFAULT_STRIPE_COUNT = (-1);

class DPvzFile {
public:

#if defined(DPVZ_MPI)

  // parallel (MPI) constructor, intended to read and/or write files
  // it may also be used by distributed tools to read files
  DPvzFile(std::string name, DPvzMode mode, uint64_t g_sz, std::string majik, std::string ext, MPI_Comm c=MPI_COMM_WORLD, bool repair=false, int root=0);

#else

  // serial (non-MPI) constructor, intended for use by serial or OpenMP-threaded tools to read files
  // it may also be used by a serial code to write a file (write assumes # of ranks == 1, rank id == 0)
  DPvzFile(std::string name, DPvzMode mode, uint64_t g_sz, std::string majik, std::string ext, bool repair=false);

#endif

  ~DPvzFile();

public:

  static const char* file_distribution_env_var;
  void        set_dist_env_var(const char* var) { file_distribution_env_var = var;  }
  const char* get_dist_env_var(void)            { return file_distribution_env_var; }

  int64_t get_page_size() { return metadata->page_size; }

  // get the size of the unpadded global section
  int64_t get_global_size();

  // copy the global data to the buffer
  bool get_global_data(void* data);

  // copy data from the global section to the buffer
  bool get_global_data(void* buf, ssize_t size, off_t offset);

#if defined(DPVZ_MPI)
  // copy data in the buffer to the global section
  bool set_global_data(void* data, ssize_t size, off_t offset, int root=0);

  // copy data in the buffer to the global section
  // there must be at least get_global_size() bytes in the array
  // only get_global_size() bytes will be copied
  // the version on rank "root" is copied to all ranks and to disk
  bool set_global_data(void* data, int root=0);

#else

  // copy data in the buffer to the global section
  bool set_global_data(void* data, ssize_t size, off_t offset);

  // copy data in the buffer to the global section
  // there must be at least get_global_size() bytes in the array
  // only get_global_size() bytes will be copied
  bool set_global_data(void* data);
#endif

  // print the structure
  void fprint(FILE* fp);


  // read an existing time step...

  // get the total number of time steps currently stored in the file
  // time step indexes are contiguous integers from 0 to N-1
  int64_t get_steps();

  // get the mapping of time steps to cycle and simulation times
  // there must be get_steps() entries in the map array
  bool get_map(DPvzTocEntry* map);

  // get the size and offset of the data block associated with this index and rank
  DPvzRankToc get_step_rank(DPvzTocEntry& step, int32_t rank);

  // get the size and offset associated with all ranks at this time step
  // there must be map[i].ranks intries in the step_toc array
  bool get_step_toc(DPvzTocEntry& step, DPvzRankToc* step_toc);

  // get a copy of the data block associated with this time step and rank
  // "data" must point to a non-NULL buffer with at least "size" bytes
  // THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.
  // return true if the call fails, false otherwise
  bool get_data(DPvzRankToc& step_rank, void* data);


  // write a new time step...

#if defined(DPVZ_MPI)
  // note: this needs to be a collective operation to get the rank id, 
  // total # of ranks, size of each block, and total data size
  // root determines the values for cycle and time
  bool write(int64_t cycle, double time, const void* buf, ssize_t size, int32_t root=0);
  bool write(int64_t cycle, double time, std::string buf, int32_t root=0);
#else
  bool write(int64_t cycle, double time, const void* buf, ssize_t size);
  bool write(int64_t cycle, double time, std::string buf);
#endif

  // truncate the file from a specific time step onward. the indicated time step and all subsequent
  // time steps will be removed. the parameter is the time step index, NOT the cycle number.
  // e.g., truncate(0) empties the file of all time steps.
  // note: the parallel version is a collective operation.
#if defined(DPVZ_MPI)
  bool truncate(int64_t idx, int32_t root=0);
#else
  bool truncate(int64_t idx);
#endif

  // truncate the file from a specific time step onward. the indicated time step and all subsequent
  // time steps will be removed. the parameters are the cycle number and simulation time. all time 
  // steps whose cycle number or simulation time are equal to or later than EITHER of the parameter 
  // cycle number or simulation time will be removed. e.g., 
  // (1) truncate(0,0) empties the file of all time steps
  // (2) truncate(500, DBL_MAX) removes all time steps including and after cycle 500
  // (3) truncate(LONG_MAX, 0.25) removes all time steps including and after simulation time 0.25
  // (4) truncate(500, 0.25) removes all time steps starting from the earlier of cycle 500 or time 0.25
  // note: the parallel version is a collective operation.
  // note: this is a convenience function for truncate(int64_t idx)
#if defined(DPVZ_MPI)
  bool truncate(int64_t cycle, double time, int32_t root=0);
#else
  bool truncate(int64_t cycle, double time);
#endif

  int64_t active_entries() { return metadata->active_entries; }

  static int rank;
  static int number_of_ranks;
  std::string file_name;		// name of the open file

  bool       failed() { return (_err_stat != DPvzNone); }
protected:
private:

  // initialize a new DPvzFile, called by constructors
  bool init(std::string name, DPvzMode m, uint64_t g_sz, std::string majik, std::string ext);

  bool    read();			// read the global data, table of contents, and metadata from a file
  bool    write();			// write the global data, table of contents, and metadata to a file
  bool    validate(uint64_t g_sz, std::string majik, bool repair=false, int root=0);	// validate an existing file

  DPvzMode    mode;			// DPvzReadOnly, DPvzReadWrite, or DPvzCreate

  int _err_stat;			// values from DPvzErr
  int _err_line;			// line number of most recent error

  bool comm_is_active;			// indicates whether in serial/threaded or parallel mode
#if defined(DPVZ_MPI)
  MPI_Comm comm;			// MPI communicator, only valid if comm_is_active == true
#endif

  int fd;				// file descriptor for the open file

  DPvzMetadata* metadata;		// copy of the file metadata section
  DPvzToc*      toc;			// copy of the file table of contents 
  DPvzGlobal*   global;			// copy of the file global section
};

#endif
