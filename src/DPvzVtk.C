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

#include <float.h>
#include <limits.h>
#include <stdlib.h>

#include <queue>

#include "DPvzVtk.h"
#include "DPvzVtkData.h"

#include "DPvzUtil.h"

/******************************************************************************/
/* a DPVTK file is a specialized archive that organizes its contents in a     */
/* convenient way for explicit computer simulations. in this case, all data   */
/* is organized first by the simulation time step, then by MPI rank, then by  */
/* file name. the time step is identified by its simulation cycle and time.   */
/* simulation cycle and time can be any value, but the cycle is a 64-bit      */
/* integer and the time is a 64-bit floating-point value. furthermore, both   */
/* values are expected to be both unique and monotonic.                       */
/*                                                                            */
/* archives are designed to be created by a distributed parallel program that */
/* runs on a large cluster. (DP in DPVTK is an abbreviation for Distributed   */
/* Parallel.) the parallelism is modeled after MPI ranks, so each time step   */
/* has contributions from some number of ranks. now, because archives may be  */
/* created by one job and have additions from other jobs, one time step may   */
/* have a different number of ranks than another time step in the file.       */
/* furthermore, some ranks may not have any data to write to the time step,   */
/* so some ranks may have empty entries.                                      */
/*                                                                            */
/* those ranks that have non-empty entries have their entries organized into  */
/* individual files, and each rank may write multiple files within its data.  */
/* each file contains a header at the beginning of the file and a footer at   */
/* the end of the file. the header is of the form:                            */
/*                                                                            */
/*    <FILE NAME='...'>\n                                                     */
/*                                                                            */
/* where the NAME field contains the desired name of the file, the tags are   */
/* case sensitive, and the single quotes are required. the file name may or   */
/* may not include one or more directory names as well, as in:                */
/*                                                                            */
/*     directory/file                                                         */
/*                                                                            */
/* all files are terminated with:                                             */
/*                                                                            */
/*    </FILE NAME='...'>\n                                                    */
/*                                                                            */
/* as before, the tags are case sensitive and single quotes are required.     */
/******************************************************************************/

#if defined(DPVZ_MPI)

// parallel (MPI) constructor, intended to read and/or write files
// it may also be used by distributed tools to read files
DPvzVtk::DPvzVtk(std::string name, DPvzMode mode, MPI_Comm c, bool repair) 
: DPvzFile(name, mode, DPVTK_GLOBAL_SIZE, DPVTK_MAJIK, DPVTK_EXT, c, repair)
{
#if defined(DPvzTrace)
  fprintf(stdout, "%s: %4d: DPvzVtk: entering and leaving\n", name_only(__FILE__), __LINE__);
#endif
}

#else

// serial (non-MPI) constructor, intended for use by serial or OpenMP-threaded tools to read files
// it may also be used by a serial code to write a file (write assumes # of ranks == 1, rank id == 0)
DPvzVtk::DPvzVtk(std::string name, DPvzMode mode, bool repair)
: DPvzFile(name, mode, DPVTK_GLOBAL_SIZE, DPVTK_MAJIK, DPVTK_EXT, repair)
{
#if defined(DPvzTrace)
  fprintf(stdout, "%s: %4d: DPvzVtk: entering and leaving\n", name_only(__FILE__), __LINE__);
#endif
}

#endif

DPvzVtk::~DPvzVtk()
{
#if defined(DPvzTrace)
  fprintf(stdout, "%s: %4d: ~DPvzVtk: entering and leaving\n", name_only(__FILE__), __LINE__);
#endif
}

struct step_rank_pair_s {
  int32_t step;
  int32_t rank;
};

/******************************************************************************/
/*                                                                            */
/* list                                                                       */
/*                                                                            */
/*    list the contents of a DPVTK archive                                    */
/*                                                                            */
/*    file         - name of the DPVTK archive                                */
/*                                                                            */
/*    cycle_start  - first cycle from which to obtain files                   */
/*                                                                            */
/*    cycle_end    - last cycle (inclusive) from which to obtain files        */
/*                                                                            */
/*    cycle_stride - cycle stride (see below)                                 */
/*                                                                            */
/*    time_start   - earliest simulation time from which to obtain files      */
/*                                                                            */
/*    time_end     - latest simulation time from which to obtain files        */
/*                                                                            */
/* the list function selects a subset of time steps and expands the files     */
/* contained within each MPI rank. time steps are included in the subset when */
/* they meet the following criteria:                                          */
/*                                                                            */
/*    cycle_start <= step.cycle && step.cycle <= cycle_end &&                 */
/*    (step.cycle % cycle_stride) == 0 &&                                     */
/*    time_start <= step.time && step.time <= time_end                        */
/*                                                                            */
/* the default value for cycle_start is -oo, cycle_end is +oo, and            */
/* cycle_stride is 1. similarly, time_start is -oo and time_end is +oo. in    */
/* other words, all time steps are selected unless the caller provides        */
/* additional qualifiers.                                                     */
/*                                                                            */
/* data is written to the FILE pointer fp.                                    */
/*                                                                            */
/* return value is either 0 on success or -1 on failure.                      */
/*                                                                            */
/******************************************************************************/

int DPvzVtk::list(FILE* fp, const char* file, const char* path, int64_t cycle_start, int64_t cycle_end, int64_t cycle_stride, double time_start, double time_end)
{
  int my_rank = 0;
  int my_size = 1;

#if defined(DPVZ_MPI)
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &my_size);
#endif

  // check whether the file exists
  int fd = open(file, O_RDONLY);
  if (fd < 0) {
    // the file does not exist, so bail...
    fprintf(stderr, "%s: %4d: file '%s' does not exist\n", __FILE__, __LINE__, file);
    return -1;
  }
  close(fd);

  // file exists, so open it
  DPvzVtk* dpvz = new DPvzVtk(file, DPvzReadOnly);

  // get the table of contents containing all of the time steps
  int time_steps = dpvz->get_steps();
  if (0 < time_steps) {
    // get a table of contents containing a list of all ranks who participated in each time step, sizes, etc.
    DPvzTocEntry* map = new DPvzTocEntry[time_steps];
    if (dpvz->get_map(map)) {
      fprintf(stderr, "%s: %4d: get_map failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
      return -1;
    }

    std::queue<struct step_rank_pair_s*> queue;

    // step through every time step
    for (int step=0; step < time_steps; step++) {
      // does this time step meet the criteria to be viewed or extracted?
      if (cycle_start <= map[step].cycle && map[step].cycle <= cycle_end && (map[step].cycle % cycle_stride) == 0 && time_start <= map[step].time && map[step].time <= time_end) {
	// get the list of ranks who participated in this time step
	int32_t ranks = map[step].ranks;

	// step through every rank in this time step
	for (int rank=0; rank < ranks; rank++) {
	  struct step_rank_pair_s* pair = new struct step_rank_pair_s;
	  pair->step = step;
	  pair->rank = rank;

	  queue.push(pair);
	}
      }
    }

    // transfer the step/rank pair from the queue to an array
    // this will be the array we use to schedule work across threads/ranks
    // every rank computes its own copy of the same list of work
    int len = queue.size();
    struct step_rank_pair_s** ppair = new struct step_rank_pair_s* [len];
    for (int i=0; i < len; i++) {
      ppair[i] = queue.front();
      queue.pop();
    }

#if defined(DPVZ_OMP)
    omp_lock_t dpvz_lock;
    omp_init_lock(&dpvz_lock);
#endif

    // do the work, but let OpenMP distribute it across threads
    int i;
#if defined(DPVZ_OMP)
#pragma omp parallel for default(none) private(i) shared(len, ppair, map, dpvz, stderr, path, dpvz_lock, stdout, my_rank, my_size)
#endif
    for (i=0; i < len; i++) {
      int step = ppair[i]->step;
      int rank = ppair[i]->rank;
      delete ppair[i];

      // if this work isn't mine, skip to the next index
      if ((i % my_size) != my_rank) continue;

      int32_t ranks = map[step].ranks;
      DPvzRankToc* step_toc = new DPvzRankToc[ranks];

#if defined(DPVZ_OMP)
      omp_set_lock(&dpvz_lock);
#endif
      bool fail = (step_toc == NULL) || dpvz->get_step_toc(map[step], step_toc);
#if defined(DPVZ_OMP)
      omp_unset_lock(&dpvz_lock);
#endif

      if (fail) {
	fprintf(stderr, "%s: %4d: get_step_toc failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
	continue;
      } else {
	// get the (inflated) data associated with this time step and rank
	char* data = new char[step_toc[rank].inflated_size];

#if defined(DPVZ_OMP)
	omp_set_lock(&dpvz_lock);
#endif
	fail = (data == NULL) || dpvz->get_data(step_toc[rank], data);
#if defined(DPVZ_OMP)
	omp_unset_lock(&dpvz_lock);
#endif

	if (fail) {
	  fprintf(stderr, "%s: %4d: get_data failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
	  continue;
	} else {
	  // the rank data contains one or more files in the archive,
	  // obtain a list of every file in the rank data
	  int count = 0;
	  DPvzVtkData* array = NULL;
	  fail = DPvzVtkData::extract(data, step_toc[rank].inflated_size, count, array);
	  if (fail) {
	    fprintf(stderr, "%s: %4d: DPvzVtkData::extract failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
	    continue;
	  } else {
	    // walk through every file in this rank and time step
	    for (int c=0; c < count; c++) {
	      // extract the file from the archive
	      // prep the name
	      DPvzVtkData elt = array[c];
	      elt.name = elt.path = elt.file = NULL;
	      fail = DPvzVtkData::name_to_vtk_data(array[c].name, path, &elt);
	      if (fail) {
		fprintf(stderr, "%s: %4d: DPvzVtkData::name_to_vtk_data failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
		continue;
	      } else {
		// print the file and directories where it resides
		// TODO DMP
		fprintf(fp, "%12d %-s\n", elt.size, elt.name);
	      }
	    }
	  }

	  if (array != NULL) {
	    DPvzVtkData::free(count, array);
	  }
	}

	if (data != NULL) {
	  delete [] data;
	  data = NULL;
	}
      }

      if (step_toc != NULL) {
	delete [] step_toc;
	step_toc = NULL;
      }
    }

#if defined(DPVZ_OMP)
    omp_destroy_lock(&dpvz_lock);
#endif

    if (ppair != NULL) {
      delete [] ppair;
      ppair = NULL;
    }

    delete [] map;
  }

  delete dpvz;

  return 0;
}


int DPvzVtk::show(FILE* fp, const char* file, const char* path, int64_t cycle_start, int64_t cycle_end, int64_t cycle_stride, double time_start, double time_end, bool crc)
{
  int my_rank = 0;
  int my_size = 1;

  int64_t total_deflated_size = 0L;
  int64_t total_inflated_size = 0L;
  int64_t total_size_on_disk  = 0L;

#if defined(DPVZ_MPI)
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &my_size);
#endif

  // check whether the file exists
  int fd = open(file, O_RDONLY);
  if (fd < 0) {
    // the file does not exist, so bail...
    fprintf(stderr, "%s: %4d: file '%s' does not exist\n", __FILE__, __LINE__, file);
    return -1;
  }
  close(fd);

  // file exists, so open it
  DPvzVtk* dpvz = new DPvzVtk(file, DPvzReadOnly);

  // get the table of contents containing all of the time steps
  int time_steps = dpvz->get_steps();
  int32_t ranks = 0;
  if (0 < time_steps) {
    // get a table of contents containing a list of all ranks who participated in each time step, sizes, etc.
    DPvzTocEntry* map = new DPvzTocEntry[time_steps];
    if (dpvz->get_map(map)) {
      fprintf(stderr, "%s: %4d: get_map failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
      return -1;
    }

    std::queue<struct step_rank_pair_s*> queue;

    // step through every time step
    for (int step=0; step < time_steps; step++) {
      // does this time step meet the criteria to be viewed or extracted?
      if (cycle_start <= map[step].cycle && map[step].cycle <= cycle_end && (map[step].cycle % cycle_stride) == 0 && time_start <= map[step].time && map[step].time <= time_end) {
	// get the list of ranks who participated in this time step
	ranks = map[step].ranks;

	// step through every rank in this time step
	for (int rank=0; rank < ranks; rank++) {
	  struct step_rank_pair_s* pair = new struct step_rank_pair_s;
	  pair->step = step;
	  pair->rank = rank;

	  queue.push(pair);
	}
      }
    }

    // transfer the step/rank pair from the queue to an array
    // this will be the array we use to schedule work across threads/ranks
    // every rank computes its own copy of the same list of work
    int len = queue.size();
    struct step_rank_pair_s** ppair = new struct step_rank_pair_s* [len];
    for (int i=0; i < len; i++) {
      ppair[i] = queue.front();
      queue.pop();
    }

#if defined(DPVZ_OMP)
    omp_lock_t dpvz_lock;
    omp_init_lock(&dpvz_lock);
#endif

    // do the work, but let OpenMP distribute it across threads
    int i;
#if defined(DPVZ_OMP)
#pragma omp parallel for default(none) private(i) shared(len, ppair, map, dpvz, stderr, path, dpvz_lock, stdout, my_rank, my_size)
#endif
    for (i=0; i < len; i++) {
      int step = ppair[i]->step;
      int rank = ppair[i]->rank;
      delete ppair[i];
      ppair[i] = NULL;

      // if this work isn't mine, skip to the next index
      if ((i % my_size) != my_rank) continue;

      int32_t ranks = map[step].ranks;
      DPvzRankToc* step_toc = new DPvzRankToc[ranks];

#if defined(DPVZ_OMP)
      omp_set_lock(&dpvz_lock);
#endif
      bool fail = (step_toc == NULL) || dpvz->get_step_toc(map[step], step_toc);
#if defined(DPVZ_OMP)
      omp_unset_lock(&dpvz_lock);
#endif

      if (fail) {
	fprintf(stderr, "%s: %4d: get_step_toc failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
	continue;
      } else {

	// get the (inflated) data associated with this time step and rank
	char* data = new char[step_toc[rank].inflated_size];

#if defined(DPVZ_OMP)
	omp_set_lock(&dpvz_lock);
#endif

	if (crc) {
	  fail = (data == NULL) || dpvz->get_data(step_toc[rank], data);
	  if (data != NULL) {
	    delete [] data;
	    data = NULL;
	  }
	} else {
	  fail = false;
	}

#if defined(DPVZ_OMP)
	omp_unset_lock(&dpvz_lock);
#endif

	if (fail) {
	  fprintf(stderr, "%s: %4d: get_data failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
	  continue;
	} else {

#if defined(DPVZ_OMP)
      omp_set_lock(&dpvz_lock);
#endif
int64_t page_size = dpvz->get_page_size();
int64_t size_on_disk = page_size * ((step_toc[rank].deflated_size + page_size - 1) / page_size);
if (rank % 1000 == 0 || (rank+1) == map[step].ranks) {
fprintf(fp, "cycle=%3d ",          map[step].cycle);
fprintf(fp, "time=%.6e ",          map[step].time);
fprintf(fp, "rank=%d/%d ",         rank, map[step].ranks);
fprintf(fp, "inflated size=%lld ", step_toc[rank].inflated_size);
fprintf(fp, "deflated size=%lld ", step_toc[rank].deflated_size);
fprintf(fp, "size on disk=%lld ",  size_on_disk);
fprintf(fp, "page size=%lld ",     page_size);
fprintf(fp, "file=%s ",            file);
fprintf(fp, "                      \r");
fflush(fp);
}

total_inflated_size += step_toc[rank].inflated_size;
total_deflated_size += step_toc[rank].deflated_size;
total_size_on_disk  += size_on_disk;
#if defined(DPVZ_OMP)
      omp_unset_lock(&dpvz_lock);
#endif
	}
      }

      if (step_toc != NULL) {
	delete [] step_toc;
	step_toc = NULL;
      }
    }

#if defined(DPVZ_OMP)
    omp_destroy_lock(&dpvz_lock);
#endif

    if (ppair != NULL) {
      for (int i=0; i < len; i++) {
	if (ppair[i] != NULL) {
	  delete ppair[i];
	  ppair[i] = NULL;
	}
      }
      delete [] ppair;
      ppair = NULL;
    }

    if (map != NULL) {
      delete [] map;
      map = NULL;
    }
  }

fprintf(fp, "total inflated size=%lld ", total_inflated_size);
fprintf(fp, "total deflated size=%lld ", total_deflated_size);
fprintf(fp, "total size on disk=%lld ",  total_size_on_disk);
fprintf(fp, "page size=%lld ",           dpvz->get_page_size());
fprintf(fp, "ranks=%d ",                 ranks);
fprintf(fp, "time steps=%d ",            time_steps-1);		// not sure why it's time_steps-1, it just gives the right answer?
fprintf(fp, "file=%s ",                  file);
fprintf(fp, "                                           \n");

  delete dpvz;

  return 0;
}

/******************************************************************************/
/*                                                                            */
/* extract                                                                    */
/*                                                                            */
/*    expand a DPVTK archive into a subset of its contained files.            */
/*                                                                            */
/*    file         - name of the DPVTK archive                                */
/*                                                                            */
/*    cycle_start  - first cycle from which to obtain files                   */
/*                                                                            */
/*    cycle_end    - last cycle (inclusive) from which to obtain files        */
/*                                                                            */
/*    cycle_stride - cycle stride (see below)                                 */
/*                                                                            */
/*    time_start   - earliest simulation time from which to obtain files      */
/*                                                                            */
/*    time_end     - latest simulation time from which to obtain files        */
/*                                                                            */
/* a DPVTK file is a specialized archive that organizes its contents in a     */
/* convenient way for explicit computer simulations. in this case, all data   */
/* is organized first by the simulation time step, then by MPI rank, then by  */
/* file name. the time step is identified by its simulation cycle and time.   */
/* simulation cycle and time can be any value, but the cycle is a 64-bit      */
/* integer and the time is a 64-bit floating-point value. furthermore, both   */
/* values are expected to be both unique and monotonic.                       */
/*                                                                            */
/* archives are designed to be created by a distributed parallel program that */
/* runs on a large cluster. (DP in DPVTK is an abbreviation for Distributed   */
/* Parallel.) the parallelism is modeled after MPI ranks, so each time step   */
/* has contributions from some number of ranks. now, because archives may be  */
/* created by one job and have additions from other jobs, one time step may   */
/* have a different number of ranks than another time step in the file.       */
/* furthermore, some ranks may not have any data to write to the time step,   */
/* so some ranks may have empty entries.                                      */
/*                                                                            */
/* those ranks that have non-empty entries have their entries organized into  */
/* individual files. each file contains a header at the beginning of the file */
/* and a footer at the end of the file. the header is of the form:            */
/*                                                                            */
/*    <FILE NAME='...'>                                                       */
/*                                                                            */
/* where the NAME field contains the desired name of the file, the tags are   */
/* case sensitive, and the single quotes are required. the file name may or   */
/* may not include one or more directory names as well, as in:                */
/*                                                                            */
/*     directory/file                                                         */
/*                                                                            */
/* all files are terminated with:                                             */
/*                                                                            */
/*    </FILE NAME='...'>                                                      */
/*                                                                            */
/* as before, the tags are case sensitive and single quotes are required.     */
/*                                                                            */
/* the extract function selects a subset of time steps and expands the files  */
/* contained within each MPI rank. time steps are included in the subset when */
/* they meet the following criteria:                                          */
/*                                                                            */
/*    cycle_start <= step.cycle && step.cycle <= cycle_end &&                 */
/*    (step.cycle % cycle_stride) == 0 &&                                     */
/*    time_start <= step.time && step.time <= time_end                        */
/*                                                                            */
/* the default value for cycle_start is -oo, cycle_end is +oo, and            */
/* cycle_stride is 1. similarly, time_start is -oo and time_end is +oo. in    */
/* other words, all time steps are selected unless the caller provides        */
/* additional qualifiers.                                                     */
/*                                                                            */
/******************************************************************************/

int DPvzVtk::extract(const char* file, const char* path, int64_t cycle_start, int64_t cycle_end, int64_t cycle_stride, double time_start, double time_end)
{
  int my_rank = 0;
  int my_size = 1;

#if defined(DPVZ_MPI)
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &my_size);
#endif

  // check whether the file exists
  int fd = open(file, O_RDONLY);
  if (fd < 0) {
    // the file does not exist, so bail...
    fprintf(stderr, "%s: %4d: file '%s' does not exist\n", __FILE__, __LINE__, file);
    return -1;
  }
  close(fd);

  // file exists, so open it
  DPvzVtk* dpvz = new DPvzVtk(file, DPvzReadOnly);

  // get the table of contents containing all of the time steps
  int time_steps = dpvz->get_steps();
  if (0 < time_steps) {
    // get a table of contents containing a list of all ranks who participated in each time step, sizes, etc.
    DPvzTocEntry* map = new DPvzTocEntry[time_steps];
    if (dpvz->get_map(map)) {
      fprintf(stderr, "%s: %4d: get_map failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
      return -1;
    }

    std::queue<struct step_rank_pair_s*> queue;

    // step through every time step
    for (int step=0; step < time_steps; step++) {
      // does this time step meet the criteria to be viewed or extracted?
      if (cycle_start <= map[step].cycle && map[step].cycle <= cycle_end && (map[step].cycle % cycle_stride) == 0 && time_start <= map[step].time && map[step].time <= time_end) {
	// get the list of ranks who participated in this time step
	int32_t ranks = map[step].ranks;

	// step through every rank in this time step
	for (int rank=0; rank < ranks; rank++) {
	  struct step_rank_pair_s* pair = new struct step_rank_pair_s;
	  pair->step = step;
	  pair->rank = rank;

	  queue.push(pair);
	}
      }
    }

    // transfer the step/rank pair from the queue to an array
    // this will be the array we use to schedule work across threads/ranks
    // every rank computes its own copy of the same list of work
    int len = queue.size();
    struct step_rank_pair_s** ppair = new struct step_rank_pair_s* [len];
    for (int i=0; i < len; i++) {
      ppair[i] = queue.front();
      queue.pop();
    }

#if defined(DPVZ_OMP)
    omp_lock_t dpvz_lock;
    omp_init_lock(&dpvz_lock);
#endif

    // do the work, but let OpenMP distribute it across threads
    int i;
#if defined(DPVZ_OMP)
#pragma omp parallel for default(none) private(i) shared(len, ppair, map, dpvz, stderr, path, dpvz_lock, stdout, my_rank, my_size)
#endif
    for (i=0; i < len; i++) {
      int step = ppair[i]->step;
      int rank = ppair[i]->rank;
      delete ppair[i];

      // if this work isn't mine, skip to the next index
      if ((i % my_size) != my_rank) continue;

      int32_t ranks = map[step].ranks;
      DPvzRankToc* step_toc = new DPvzRankToc[ranks];

#if defined(DPVZ_OMP)
      omp_set_lock(&dpvz_lock);
#endif
      bool fail = (step_toc == NULL) || dpvz->get_step_toc(map[step], step_toc);
#if defined(DPVZ_OMP)
      omp_unset_lock(&dpvz_lock);
#endif

      if (fail) {
	fprintf(stderr, "%s: %4d: get_step_toc failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
	continue;
      } else {
	// get the (inflated) data associated with this time step and rank
	char* data = new char[step_toc[rank].inflated_size];

#if defined(DPVZ_OMP)
	omp_set_lock(&dpvz_lock);
#endif
	fail = (data == NULL) || dpvz->get_data(step_toc[rank], data);
#if defined(DPVZ_OMP)
	omp_unset_lock(&dpvz_lock);
#endif

	if (fail) {
	  fprintf(stderr, "%s: %4d: get_data failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
	  continue;
	} else {
	  // the rank data contains one or more files in the archive,
	  // obtain a list of every file in the rank data
	  int count = 0;
	  DPvzVtkData* array = NULL;
	  fail = DPvzVtkData::extract(data, step_toc[rank].inflated_size, count, array);
	  if (fail) {
	    fprintf(stderr, "%s: %4d: DPvzVtkData::extract failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
	    continue;
	  } else {
	    // walk through every file in this rank and time step
	    for (int c=0; c < count; c++) {
	      // extract the file from the archive
	      // prep the name
	      DPvzVtkData elt = array[c];
	      elt.name = elt.path = elt.file = NULL;
	      fail = DPvzVtkData::name_to_vtk_data(array[c].name, path, &elt);
	      if (fail) {
		fprintf(stderr, "%s: %4d: DPvzVtkData::name_to_vtk_data failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
		continue;
	      } else {
		// create the file and directories where it resides
		fail = DPvzVtkData::create(&elt);
		if (fail) {
		  fprintf(stderr, "%s: %4d: DPvzVtkData::create failed, file '%s'\n", __FILE__, __LINE__, dpvz->file_name.c_str());
		  continue;
		}
	      }
	    }
	  }

	  if (array != NULL) {
	    DPvzVtkData::free(count, array);
	  }
	}

	if (data != NULL) {
	  delete [] data;
	  data = NULL;
	}
      }

      if (step_toc != NULL) {
	delete [] step_toc;
	step_toc = NULL;
      }
    }

#if defined(DPVZ_OMP)
    omp_destroy_lock(&dpvz_lock);
#endif

    if (ppair != NULL) {
      delete [] ppair;
      ppair = NULL;
    }

    delete [] map;
  }

  delete dpvz;

  return 0;
}

/******************************************************************************/
/*                                                                            */
/* memrchar                                                                   */
/*                                                                            */
/* return a pointer to the right-most occurence of the character c, or NULL   */
/* if there isn't one.                                                        */
/*                                                                            */
/* this function compensates for the fact that some compilers do not provide  */
/* the GNU function memrchr.                                                  */
/*                                                                            */
/******************************************************************************/
inline static const char* memrchar(const char* str, char c, int len)
{
  if (str != NULL && 0 < len) {
    for (int i=len-1; 0 <= i; i--) {
      if (str[i] == c) {
        return str+i;
      }
    }
  }

  return NULL;
}


/******************************************************************************/
/*                                                                            */
/* main                                                                       */
/*                                                                            */
/* main options files...                                                      */
/*                                                                            */
/* # common options                                                           */
/*   -l  or --list                      show file names and sizes             */
/*   -s  or --show                      show detailed file names and sizes    */
/*   -x  or --extract                   extract files from the archive        */
/*   -o  or --output       <path>       output directory or path              */
/*   -v  or --verbose                   show more details                     */
/*                                                                            */
/* # less common options                                                      */
/*   -cs or --cycle-start  <cycle>      starting cycle number                 */
/*   -ce or --cycle-end    <cycle>      ending cycle number                   */
/*   -st or --cycle-stride <stride>     cycle stride                          */
/*   -ts or --time-start   <time>       starting simulation time              */
/*   -te or --time-end     <time>       ending simulation time                */
/*                                                                            */
/* TODO                                                                       */
/*   -n  or -name          <file>       archived file name                    */
/*                                                                            */
/******************************************************************************/

int DPvzVtk::main(int argc, char**argv)
{
#if defined(DPVZ_MPI)
  MPI_Init(&argc, &argv);
#endif

  bool    list         = false;		// --list
  bool    show         = false;		// --show
  bool    crc          = false;		// --crc
  bool    extract      = false;		// --extract
  bool    verbose      = false;		// --verbose
  int64_t cycle_start  = LLONG_MIN;	// --cycle-start  <cycle>
  int64_t cycle_end    = LLONG_MAX;	// --cycle-end    <cycle>
  int64_t cycle_stride = 1;		// --cycle-stride <stride>
  double  time_start   = -DBL_MAX;	// --time-start   <time>
  double  time_end     = DBL_MAX;	// --time-end     <time>
  char*   path         = NULL;		// --output <path>
  char*   cwd          = new char[4096];

  cwd = getcwd(cwd, 4096);

  bool help = false;
  for (int ac=1; ac < argc; ac++) {
    char* av  = argv[ac];
    char* dot = (char*) memrchar((char*) av, '.', strlen(av));
    bool  av_ends_with_dpvtk = (dot == NULL) ? false : (strcasecmp(dot, "." DPVTK_EXT) == 0);

    if (       (strcasecmp(argv[ac], "-cs") == 0 || strcasecmp(argv[ac], "--cycle-start")  == 0) && (ac+1) < argc) {
      cycle_start = atol(argv[++ac]);
      if (verbose) fprintf(stdout, "%s: %4d: cycle_start=%lld\n", __FILE__, __LINE__, cycle_start);
    } else if ((strcasecmp(argv[ac], "-ce") == 0 || strcasecmp(argv[ac], "--cycle-end")    == 0) && (ac+1) < argc) {
      cycle_end = atol(argv[++ac]);
      if (verbose) fprintf(stdout, "%s: %4d: cycle_end=%lld\n", __FILE__, __LINE__, cycle_end);
    } else if ((strcasecmp(argv[ac], "-st") == 0 || strcasecmp(argv[ac], "--cycle-stride") == 0) && (ac+1) < argc) {
      cycle_stride = atol(argv[++ac]);
      if (verbose) fprintf(stdout, "%s: %4d: cycle_stride=%lld\n", __FILE__, __LINE__, cycle_stride);
    } else if ((strcasecmp(argv[ac], "-ts") == 0 || strcasecmp(argv[ac], "--time-start")   == 0) && (ac+1) < argc) {
      time_start = atof(argv[++ac]);
      if (verbose) fprintf(stdout, "%s: %4d: time_start=%f\n", __FILE__, __LINE__, time_start);
    } else if ((strcasecmp(argv[ac], "-te") == 0 || strcasecmp(argv[ac], "--time-end")     == 0) && (ac+1) < argc) {
      time_end = atof(argv[++ac]);
      if (verbose) fprintf(stdout, "%s: %4d: time_end=%f\n", __FILE__, __LINE__, time_end);
    } else if ( strcasecmp(argv[ac], "-ls") == 0 || strcasecmp(argv[ac], "--list")         == 0 || strcasecmp(argv[ac], "-l" ) == 0) {
      list = true;
      show = false;
      if (verbose) fprintf(stdout, "%s: %4d: list\n", __FILE__, __LINE__);
    } else if ( strcasecmp(argv[ac], "-sh") == 0 || strcasecmp(argv[ac], "--show")         == 0 || strcasecmp(argv[ac], "-s" ) == 0) {
      show = true;
      list = false;
      if (verbose) fprintf(stdout, "%s: %4d: show\n", __FILE__, __LINE__);
    } else if ( strcasecmp(argv[ac], "--crc") == 0 || strcasecmp(argv[ac], "-c" ) == 0) {
      crc = true;
      if (verbose) fprintf(stdout, "%s: %4d: crc\n", __FILE__, __LINE__);
    } else if ( strcasecmp(argv[ac], "-ex") == 0 || strcasecmp(argv[ac], "--extract")      == 0 || strcasecmp(argv[ac], "-x" ) == 0) {
      extract = true;
      if (verbose) fprintf(stdout, "%s: %4d: extract\n", __FILE__, __LINE__);
    } else if ((strcasecmp(argv[ac], "-ou") == 0 || strcasecmp(argv[ac], "--output")       == 0 || strcasecmp(argv[ac], "-o" ) == 0 || strcasecmp(argv[ac], "-out" ) == 0 || strcasecmp(argv[ac], "--out" ) == 0) && (ac+1) < argc) {
      path = argv[++ac];
      if (verbose) fprintf(stdout, "%s: %4d: output='%s'\n", __FILE__, __LINE__, path);
    } else if ( strcasecmp(argv[ac], "-ve") == 0 || strcasecmp(argv[ac], "--verbose")      == 0 || strcasecmp(argv[ac], "-v" ) == 0) {
      verbose = true;
    } else if (av_ends_with_dpvtk) {
      if (list) {
	DPvzVtk::list(stdout, argv[ac], path, cycle_start, cycle_end, cycle_stride, time_start, time_end);
      } else if (show) {
	DPvzVtk::show(stdout, argv[ac], path, cycle_start, cycle_end, cycle_stride, time_start, time_end, crc);
      } else if (! show && ! list && ! extract && crc) {
	DPvzVtk::show(stdout, argv[ac], path, cycle_start, cycle_end, cycle_stride, time_start, time_end, crc);
      }
      if (extract) {
	DPvzVtk::extract(argv[ac], path, cycle_start, cycle_end, cycle_stride, time_start, time_end);
      }
    } else {
      if ( ! (strcasecmp(argv[ac], "-he") == 0 || strcasecmp(argv[ac], "--help") == 0 || strcasecmp(argv[ac], "-h" ) == 0 || strcasecmp(argv[ac], "-help" ) == 0)) {
	fprintf(stderr, "%s: %4d: unrecognized option '%s'\n", __FILE__, __LINE__, argv[ac]);
	fprintf(stderr, "\n");
      }
      help = true;
      break;
    }
  }

  if (help || argc == 1) {
    fprintf(stderr, "dpvz-ar options files...\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "# common options\n");
    fprintf(stderr, "  -l  or --list                             show file names and sizes\n");
    fprintf(stderr, "  -s  or --show                             show detailed file names and sizes\n");
    fprintf(stderr, "  -c  or --crc                              perform CRC checks on all data (--show)\n");
    fprintf(stderr, "  -x  or --extract                          extract files from the archive\n");
    fprintf(stderr, "  -o  or --output       <path>              output directory or path\n");
    fprintf(stderr, "  -v  or --verbose                          show more details\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "# less common options\n");
    fprintf(stderr, "  -cs or --cycle-start  <cycle>             starting cycle number\n");
    fprintf(stderr, "  -ce or --cycle-end    <cycle>             ending cycle number\n");
    fprintf(stderr, "  -st or --cycle-stride <stride>            cycle stride\n");
    fprintf(stderr, "  -ts or --time-start   <time>              starting simulation time\n");
    fprintf(stderr, "  -te or --time-end     <time>              ending simulation time\n");

    return -1;
  }

#if defined(DPVZ_MPI)
  MPI_Finalize();
#endif

  return 0;
}
