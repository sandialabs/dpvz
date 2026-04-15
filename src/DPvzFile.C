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

#include "DPvzFile.h"

#include <string>
#include <dirent.h>
#include <fcntl.h>
#include <float.h>
#include <poll.h>

#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


#if defined(DPVZ_MPI)
#include <mpi.h>
#endif

#include "DPvzUtil.h"


#define set_err(stat, tgt)	{ _err_stat = (_err_stat == DPvzHard) ? DPvzHard : stat; _err_line = __LINE__; goto tgt; }
#define reset_err()		{ _err_stat = (_err_stat == DPvzHard) ? DPvzHard : DPvzNone; _err_line = 0; }
#define clear_err()		{ _err_stat = DPvzNone; _err_line = 0; }

/****************************************************************************/
/*                                                                          */
/*                                 DPvzFile                                 */
/*                                                                          */
/****************************************************************************/

/****************************************************************************/
/*                                                                          */
/*                              Public Methods                              */
/*                                                                          */
/****************************************************************************/

int DPvzFile::rank = 0;
int DPvzFile::number_of_ranks = 1;
const char* DPvzFile::file_distribution_env_var = "RESTART";

#if defined(DPVZ_MPI)
/****************************************************************************/
DPvzFile::DPvzFile(std::string name, DPvzMode m, uint64_t g_sz, std::string majik, std::string ext, MPI_Comm c, bool repair, int root)
/****************************************************************************/
/*                                                                          */
/* parallel (MPI) constructor, intended for use by distributed memory app,  */
/* to write files. it may also be used by distributed tools to read files.  */
/*                                                                          */
/* THIS IS A COLLECTIVE COMMUNICATION ROUTINE. ALL RANKS MUST PARTICIPATE.  */
/*                                                                          */
/* if the file does not exist, create a new file using 'g_sz' as the size   */
/* of the global data section and open the file for write. if the file      */
/* already exists open the file for write, but check whether 'g_sz' matches */
/* the file global section unpadded size.                                   */
/*                                                                          */
/* name:   name of the file to be created/opened, including its extension   */
/* m:      mode, i.e., DPvzReadOnly, DPvzReadWrite, or DPvzCreate           */
/* g_sz:   size of the global section, if the file is created               */
/* majik:  magic sub-string specific to this file type                      */
/* ext:    file extension                                                   */
/* c:      MPI communicator                                                 */
/* repair: attempt to repair the file if it is damaged or corrupted         */
/*                                                                          */
/* CAUTION: ALL RANKS MUST USE THE SAME PARAMETER VALUES WHEN CALLING THIS  */
/* CONSTRUCTOR OR UNDEFINED BEHAVIOR WILL RESULT.                           */
/*                                                                          */
/* failed is set to true if the call fails, false otherwise.                */
/* if it fails, _err_line will be set to a non-zero value corresponding to  */
/* the line nuber where it failed.                                          */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: entering\n", name_only(__FILE__), __LINE__);
#endif

  mode = m;
  comm = c;

  comm_is_active = true;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &number_of_ranks);

  clear_err();

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: entering name='%s' mode=0x%x g_sz=%lld majik='%s' ext='%s' rank=%d size=%d\n", name_only(__FILE__), __LINE__, rank, name.c_str(), m, g_sz, majik.c_str(), ext.c_str(), rank, number_of_ranks);
#endif

  // find the file to be opened
  // all ranks must figure out the name of the function to be opened in the same way
  if (0 < ext.size()) {
    // if name ends with ext, use the name as it stands
    char cext[ext.size()+2];
    sprintf(cext, "%s", ext.c_str());
    if (cext[0] != '.') {
      // the file extension doesn't begin with '.', so add '.' to the extension
      sprintf(cext, ".%s", ext.c_str());
    }
    int len = strlen(cext);
    if ((size_t) len < name.size()) {
      // the name is larger than the file extension
      if (strcasecmp(name.c_str() + name.size() - len, cext) == 0) {
	// the name ends with .ext, so we're done
	file_name = name;
      } else {
	// the name does not end with .ext, so append the extension
	char buf[name.size() + len + 2];
	sprintf(buf, "%s%s", name.c_str(), cext);
	file_name = std::string(buf);
      }
    } else {
      // the name is shorter than the file extension, so append the extension
      char buf[name.size() + len + 2];
      sprintf(buf, "%s%s", name.c_str(), cext);
      file_name = std::string(buf);
    }
  } else {
    // there is no file extension, so just use the name as given
    file_name = name;
  }

  // sync up the file name and such across all of the ranks 
  // start with the file name
  int32_t file_name_len = file_name.size();
  MPI_Bcast(&file_name_len, 1, MPI_INT32_T, root, comm);
  char file_name_buf[file_name_len+1];
  memset(file_name_buf, '\0', sizeof(file_name_buf));
  memcpy(file_name_buf, file_name.c_str(), file_name.size());
  MPI_Bcast(file_name_buf, sizeof(file_name_buf), MPI_BYTE, root, comm);
  file_name = std::string(file_name_buf);

  // next, sync up the mode
  int32_t mode_int = (int32_t) mode;
  MPI_Bcast(&mode_int, 1, MPI_INT32_T, root, comm);
  mode = (DPvzMode) mode_int;

  // now, sync up the global size
  MPI_Bcast(&g_sz, 1, MPI_UINT64_T, root, comm);

  // finally, sync up the repair flag
  MPI_Bcast(&repair, 1, MPI_C_BOOL, root, comm);

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: file_name='%s'\n", name_only(__FILE__), __LINE__, rank, file_name.c_str());
#endif

  metadata = NULL;
  toc      = NULL;
  global   = NULL;

  // everyone now has the same file name to open
  MPI_Barrier(comm);
  if (rank == 0) {
    // does the file exist?
    fd = open(file_name.c_str(), O_RDONLY);
    bool file_exists = (0 <= fd);
    if (file_exists && close(fd) < 0) {
      // file exists but we can't close the file descriptor
      fprintf(stderr, "%s: %4d: DPvzFile: unable to close file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      perror("close");
      set_err(DPvzHard, check1);
    } else if ((! file_exists && mode == DPvzCreate) || mode == DPvzReplace) {
      if (0 < g_sz && majik.c_str() != NULL) {
	// everything is set for creating a new file, start with creating an empty file
	if (! file_exists) {
	  // file does not exist, create the file
	  if (chk_file_data_distribution(DPvzFile::file_distribution_env_var)) {
	    // file distribution is set on individual files, so create an empty file using set_file_lfs_stripe_size()
	    if (set_file_lfs_stripe_size(file_name.c_str(), DPVZ_DEFAULT_STRIPE_SIZE, DPVZ_DEFAULT_STRIPE_INDEX, DPVZ_DEFAULT_STRIPE_COUNT)) {
	      fprintf(stderr, "%s: %4d: DPvzFile: unable to set the file stripe size, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	      set_err(DPvzHard, check1);
	    }
	  } else {
	    // file distribution is not requested, so create an empty file using conventional means
	    if ((fd=creat(file_name.c_str(), S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP)) < 0) {
	      fprintf(stderr, "%s: %4d: DPvzFile: unable to create file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	      perror("creat");
	      set_err(DPvzHard, check1);
	    }

	    // flush the file to disk
	    if (fsync(fd) < 0) {
	      fprintf(stderr, "%4s: %4d: DPvzFile: unable to flush file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	      perror("fsync");
	      set_err(DPvzHard, check1);
	    }

	    // close the newly created file
	    if (close(fd) < 0) {
	      fprintf(stderr, "%s: %4d: DPvzFile: unable to close file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	      perror("close");
	      set_err(DPvzHard, check1);
	    }
	  }
	} else {
	  // file exists and needs to be truncated to the beginning, but use the current file distribution
	  fd = open(file_name.c_str(), O_WRONLY|O_TRUNC);
	  if (close(fd) < 0) {
	    // file exists but we can't close the file descriptor
	    fprintf(stderr, "%s: %4d: DPvzFile: unable to close file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	    perror("close");
	    set_err(DPvzHard, check1);
	  }
	}

	// re-open the new, empty file
	if ((fd=open(file_name.c_str(), O_RDWR)) < 0) {
	  fprintf(stderr, "%s: %4d: DPvzFile: unable to open file for write, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  perror("open");
	  set_err(DPvzHard, check1);
	}

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: creating metadata, g_sz=%ld, majik='%s'\n", name_only(__FILE__), __LINE__, rank, g_sz, majik.c_str());
#endif

	// create, initialize, and write the structure to the file
	DPvzMetadata* meta = new DPvzMetadata(g_sz, (const char*) majik.c_str());

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: writing global data, file_name='%s'\n", name_only(__FILE__), __LINE__, rank, file_name.c_str());
#endif

	DPvzGlobal* glob = new DPvzGlobal(meta);
	glob->write(fd, meta, file_name);

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: writing table of contents, file_name='%s'\n", name_only(__FILE__), __LINE__, rank, file_name.c_str());
#endif

	DPvzToc* toco = new DPvzToc();
	toco->write(fd, meta, file_name);

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: writing metadata, file_name='%s'\n", name_only(__FILE__), __LINE__, rank, file_name.c_str());
#endif

	meta->write(fd, file_name);

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: flushing to disk, file_name='%s'\n", name_only(__FILE__), __LINE__, rank, file_name.c_str());
#endif

	// flush the file to disk
	if (fsync(fd) < 0) {
	  fprintf(stderr, "%4s: %4d: DPvzFile: unable to flush file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  perror("fsync");
	  set_err(DPvzHard, check1);
	}

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: closing file descriptor %d, file_name='%s'\n", name_only(__FILE__), __LINE__, rank, fd, file_name.c_str());
#endif

	// close the file descriptor
	if (close(fd) < 0) {
	  fprintf(stderr, "%4s: %4d: DPvzFile: unable to close file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  perror("close");
	  set_err(DPvzHard, check1);
	}

	delete glob;
	glob = NULL;

	delete toco;
	toco = NULL;

	delete meta;
	meta = NULL;
      } else {
	if (0 < g_sz && majik.c_str() != NULL) {
	  fprintf(stderr, "%s: %4d: DPvzFile: attempt to create a new file with bad global size (%ld) and no majik string, file='%s'\n", name_only(__FILE__), __LINE__, g_sz, file_name.c_str());
	} else if (0 < g_sz) {
	  fprintf(stderr, "%s: %4d: DPvzFile: attempt to create a new file with bad global size (%ld), file='%s'\n", name_only(__FILE__), __LINE__, g_sz, file_name.c_str());
	} else if (majik.c_str() != NULL) {
	  fprintf(stderr, "%s: %4d: DPvzFile: attempt to create a new file with no majik string, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	}
	set_err(DPvzHard, check1);
      }
    } else if (! file_exists) {
      fprintf(stderr, "%s: %4d: DPvzFile: expected file opened for read or write does not exist, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      set_err(DPvzHard, check1);
    } else {
      // the file exists, we can close it, and the mode is not set to overwrite or replace the file
    }
  }
  fd = -1;

  // checkpoint
check1:
  MPI_Allreduce(MPI_IN_PLACE, &_err_stat, 1, MPI_INT, MPI_LOR, comm);
  if (failed()) {
    MPI_Allreduce(MPI_IN_PLACE, &_err_line, 1, MPI_INT, MPI_MAX, comm);
    fprintf(stderr, "%s: %4d: DPvzFile: error in opening, closing, or creating the file, err=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
    return;
  }

  // on some file systems, fsync is not enough to force file metadata to the file system,
  // so we explicitly do a spin-wait until every rank sees the file
  double timeout = 300;	// timeout is set to 300 seconds (5 minutes) by default
  char* timeout_str = getenv("DPVZ_TIMEOUT");
  if (timeout_str != NULL) {
    double val = atof(timeout_str);
    if (timeout_str[0] == '+' || timeout_str[0] == '-' || ('0' <= timeout_str[0] && timeout_str[0] <= '9')) {
	timeout = (val < 0) ?  DBL_MAX : val;
    }
  }
  double start = seconds();
  // char hostname[64];
  // gethostname(hostname, sizeof hostname);
  // fprintf(stdout, "%s: %4d: DPvzFile: r=%d: re-opening file_name='%s' host='%s' timeout=%f, sec=%f\n", name_only(__FILE__), __LINE__, rank, file_name.c_str(), hostname, timeout, (seconds()-start));
  bool timed_out = true;
  while ((seconds() - start) < timeout) {
    // fprintf(stdout, "%s: %4d: DPvzFile: r=%d: re-opening file_name='%s' host='%s' sec=%f\n", name_only(__FILE__), __LINE__, rank, file_name.c_str(), hostname, (seconds()-start));
#if defined(USE_STAT)
    struct stat statbuf;
    if (stat(name.c_str(), &statbuf) == 0) {
        // fprintf(stdout, "%s: %4d: DPvzFile: r=%d: file re-open succeeded, file_name='%s' host='%s' sec=%f\n", name_only(__FILE__), __LINE__, rank, file_name.c_str(), hostname, (seconds()-start));
	timed_out = false;
	break;
    }
#else
    if (0 <= (fd=open(file_name.c_str(), O_RDONLY))) {
        // fprintf(stdout, "%s: %4d: DPvzFile: r=%d: file re-open succeeded, file_name='%s' host='%s' sec=%f\n", name_only(__FILE__), __LINE__, rank, file_name.c_str(), hostname, (seconds()-start));
	close(fd);
	timed_out = false;
	break;
    }
#endif

    struct timespec sleep_time;
    sleep_time.tv_sec = 0;
    sleep_time.tv_nsec = 250000000;
    nanosleep(&sleep_time, NULL);	// sleep for sleep_time seconds 
  }

  MPI_Allreduce(MPI_IN_PLACE, &timed_out, 1, MPI_C_BOOL, MPI_LOR, comm);
  if (timed_out) {
    if (rank == 0) {
      fprintf(stderr, "%s: %4d: DPvzFile: unable to open file, operation timed out after %.2f seconds, file='%s'\n", name_only(__FILE__), __LINE__, timeout, file_name.c_str());
    }
    set_err(DPvzHard, check2);
  }

  // fprintf(stdout, "%s: %4d: DPvzFile: r=%d: entering barrier host='%s' sec=%f\n", name_only(__FILE__), __LINE__, rank, hostname, (seconds()-start)); fflush(stdout);
  // MPI_Barrier(comm);
  // fprintf(stdout, "%s: %4d: DPvzFile: r=%d: leaving barrier host='%s' sec=%f\n", name_only(__FILE__), __LINE__, rank, hostname, (seconds()-start)); fflush(stdout);

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: re-opening the file, mode=%d, file_name='%s'\n", name_only(__FILE__), __LINE__, rank, mode, file_name.c_str());
#endif

  if (mode == DPvzReadOnly) {
    if ((fd=open(file_name.c_str(), O_RDONLY)) < 0) {
      fprintf(stderr, "%s: %4d: DPvzFile: unable to open file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      perror("open");
      set_err(DPvzHard, check2);
    }
  } else if (mode == DPvzReadWrite || mode == DPvzCreate || mode == DPvzReplace) {
    if ((fd=open(file_name.c_str(), O_RDWR)) < 0) {
      fprintf(stderr, "%s: %4d: rank %d: DPvzFile: unable to open file, file='%s'\n", name_only(__FILE__), __LINE__, rank, file_name.c_str());
      perror("open");

      // DMP
      struct stat statbuf;
      if (stat(name.c_str(), &statbuf) == 0) {
        fprintf(stderr, "%s: %4d: rank %d: DPvzFile: file size='%ld'\n", name_only(__FILE__), __LINE__, rank, statbuf.st_size);
      } else {
        fprintf(stderr, "%s: %4d: rank %d: DPvzFile: unable to stat file, file='%s'\n", name_only(__FILE__), __LINE__, rank, file_name.c_str());
        perror("stat");
      }

      set_err(DPvzHard, check2);
    }
  } else {
    fprintf(stderr, "%s: %4d: DPvzFile: unknown file mode (0x%x), file='%s'\n", name_only(__FILE__), __LINE__, mode, file_name.c_str());
    set_err(DPvzHard, check2);
  }

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: read the file, file_name='%s'\n", name_only(__FILE__), __LINE__, rank, file_name.c_str());
#endif

  // read the file
  if (read()) {
    fprintf(stderr, "%s: %4d: DPvzFile: unable to read file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzHard, check2);
  }

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: validate, file_name='%s'\n", name_only(__FILE__), __LINE__, rank, file_name.c_str());
#endif

  // check the file for consistency
  if (validate(metadata->global_only, majik, repair)) {
    fprintf(stderr, "%s: %4d: DPvzFile: unable to validate file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzHard, check2);
  }

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: seek..., file_name='%s'\n", name_only(__FILE__), __LINE__, rank, file_name.c_str());
#endif

  // reset the file location to the beginning
  if (lseek(fd, 0L, SEEK_SET) < 0) {
    fprintf(stderr, "%s: %4d: DPvzFile: unable to seek, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    set_err(DPvzHard, check2);
  }

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: check for errors, file_name='%s'\n", name_only(__FILE__), __LINE__, rank, file_name.c_str());
#endif

  // checkpoint
check2:
  MPI_Allreduce(MPI_IN_PLACE, &_err_stat, 1, MPI_INT, MPI_LOR, comm);
  if (failed()) {
    MPI_Allreduce(MPI_IN_PLACE, &_err_line, 1, MPI_INT, MPI_MAX, comm);
    if (rank == 0) {
      fprintf(stderr, "%s: %4d: DPvzFile: error in opening file, line=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
    }
    return;
  }

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: r=%d: leaving\n", name_only(__FILE__), __LINE__, rank);
#endif
}

/****************************************************************************/
DPvzFile::~DPvzFile()
/****************************************************************************/
/*                                                                          */
/* close out the file. if the file was opened for write, write out any      */
/* remaining data to the file before closing.                               */
/*                                                                          */
/* THIS IS A COLLECTIVE COMMUNICATION ROUTINE.                              */
/* IT REQUIRES ALL RANKS TO PARTICIPATE.                                    */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: ~DPvzFile: entering\n", name_only(__FILE__), __LINE__);
#endif

  if (mode == DPvzReadWrite) {
    // closing the file when opened for write must first write the metadata 
    // and table of contents, at least, and possibly the global data section 
    // if it has been modified
    if (write()) {
      fprintf(stderr, "%s: %4d: DPvzFile: unable to write file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      return;
    }
  }

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: ~DPvzFile: midway\n", name_only(__FILE__), __LINE__);
#endif

  if (0 <= fd && close(fd) < 0) {
    // unable to close the file
    fprintf(stderr, "%s: %4d: ~DPvzFile: error occurred when closing '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("close");
  }
  fd = -1;

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: ~DPvzFile: leaving\n", name_only(__FILE__), __LINE__);
#endif
}

#else

/****************************************************************************/
DPvzFile::DPvzFile(std::string name, DPvzMode m, uint64_t g_sz, std::string majik, std::string ext, bool repair)
/****************************************************************************/
/*                                                                          */
/* serial (non-MPI) constructor, intended for use by serial or OpenMP-      */
/* threaded tools to read files. it may also be used by a serial code to    */
/* write a file (write assumes # of ranks == 1, rank id == 0).              */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/* if the file does not exist, create a new file using 'g_sz' as the size   */
/* of the global data section and open the file for write. if the file      */
/* already exists open the file for write, but check whether 'g_sz' matches */
/* the file global section unpadded size.                                   */
/*                                                                          */
/* name:  name of the file to be created or opened, including its extension */
/* mode:  DPvzReadOnly, DPvzReadWrite, DPvzCreate                           */
/* g_sz:  size of the global section, if the file is created                */
/* majik: secondary magic string to be used, if the file is created         */
/* ext:   file extension (is this needed?)                                  */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: entering, g_sz=%ld, majik='%s'\n", name_only(__FILE__), __LINE__, g_sz, majik.c_str());
#endif

  mode = m;
  comm_is_active = false;
  clear_err();

  // find the name of the file to be opened
  if (0 < ext.size()) {
    // if name ends with ext, use the name as it stands
    char cext[ext.size()+2];
    sprintf(cext, "%s", ext.c_str());
    if (cext[0] != '.') {
      // the file extension doesn't begin with '.', so add '.' to the extension
      sprintf(cext, ".%s", ext.c_str());
    }
    int len = strlen(cext);
    if ((size_t) len < name.size()) {
      // the name is larger than the file extension
      if (strcasecmp(name.c_str() + name.size() - len, cext) == 0) {
	// the name ends with .ext, so we're done
	file_name = name;
      } else {
	// the name does not end with .ext, so append the extension
	char buf[name.size() + len + 2];
	sprintf(buf, "%s%s", name.c_str(), cext);
	file_name = std::string(buf);
      }
    } else {
      // the name is shorter than the file extension, so append the extension
      char buf[name.size() + len + 2];
      sprintf(buf, "%s%s", name.c_str(), cext);
      file_name = std::string(buf);
    }
  } else {
    // there is no file extension, so just use the name as given
    file_name = name;
  }

  metadata = NULL;
  toc      = NULL;
  global   = NULL;

  // does the file exist?
  fd = open(file_name.c_str(), O_RDONLY);
  bool file_exists = (0 <= fd);
  if (file_exists && close(fd) < 0) {
    // file exists but we can't close the file descriptor
    fprintf(stderr, "%s: %4d: DPvzFile: unable to close file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("close");
    set_err(DPvzHard, exit);
  } else if (! file_exists) {
    // file does not exist
    if (0 < g_sz && m == DPvzCreate && majik.c_str() != NULL) {
	// create the file
	if ((fd=creat(file_name.c_str(), S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP)) < 0) {
	  fprintf(stderr, "%s: %4d: DPvzFile: unable to create file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  perror("creat");
	  set_err(DPvzHard, exit);
	}

	// close the newly created file
	if (!failed() && close(fd) < 0) {
	  fprintf(stderr, "%s: %4d: DPvzFile: unable to close file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  perror("close");
	  set_err(DPvzHard, exit);
	}

	// re-open the file
	if (!failed() && (fd=open(file_name.c_str(), O_RDWR)) < 0) {
	  fprintf(stderr, "%s: %4d: DPvzFile: unable to open file for write, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  perror("open");
	  set_err(DPvzHard, exit);
	}

	// TODO check for errors...
	// create, initialize, and write the structure to the file
	DPvzMetadata* meta = new DPvzMetadata(g_sz, (const char*) majik.c_str());

	DPvzGlobal* glob = new DPvzGlobal(meta);
	glob->write(fd, meta, file_name);

	DPvzToc* toco = new DPvzToc();
	toco->write(fd, meta, file_name);

	meta->write(fd, file_name);

	// close the file descriptor
	if (!failed() && close(fd) < 0) {
	  fprintf(stderr, "%4s: %4d: DPvzFile: unable to close file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  perror("close");
	  set_err(DPvzHard, exit);
	}
	fd = -1;

	delete glob;
	glob = NULL;

	delete toco;
	toco = NULL;

	delete meta;
	meta = NULL;

	m = DPvzReadWrite;
    } else {
      fprintf(stderr, "%s: %4d: DPvzFile: expected file does not exist, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      set_err(DPvzHard, exit);
    }
  }
  fd = -1;

  mode = m;	// mode - read or write
  if (mode == DPvzReadOnly) {
    if ((fd=open(file_name.c_str(), O_RDONLY)) < 0) {
      fprintf(stderr, "%s: %4d: DPvzFile: unable to open file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      perror("open");
      set_err(DPvzHard, exit);
    }
  } else if (mode == DPvzReadWrite) {
    if ((fd=open(file_name.c_str(), O_RDWR)) < 0) {
      fprintf(stderr, "%s: %4d: DPvzFile: unable to open file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      perror("open");
      set_err(DPvzHard, exit);
    }
  } else if (mode == DPvzCreate) {
    if ((fd=creat(file_name.c_str(), S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP)) < 0) {
      fprintf(stderr, "%s: %4d: DPvzFile: unable to create file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      perror("creat");
      set_err(DPvzHard, exit);
    }
  } else {
    fprintf(stderr, "%s: %4d: DPvzFile: unknown mode (%x), i.e., not read-only, read/write, or create\n", name_only(__FILE__), __LINE__, mode);
    set_err(DPvzHard, exit);
  }

  if (fd < 0) {
    fprintf(stderr, "%s: %4d: DPvzFile: unable to open file for read/write, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("open");
    set_err(DPvzHard, exit);
  }

  // read the file
  if (read()) {
    fprintf(stderr, "%s: %4d: DPvzFile: unable to read file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzHard, exit);
  }

  // check the file for consistency
  if (validate(metadata->global_only, majik, repair)) {
    fprintf(stderr, "%s: %4d: DPvzFile: unable to validate file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzHard, exit);
  }

  // reset the file location to the beginning
  if (lseek(fd, 0L, SEEK_SET) < 0) {
    fprintf(stderr, "%s: %4d: DPvzFile: unable to seek, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    set_err(DPvzHard, exit);
  }

exit:
  ;

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzFile: leaving\n", name_only(__FILE__), __LINE__);
#endif
}

/****************************************************************************/
DPvzFile::~DPvzFile()
/****************************************************************************/
/*                                                                          */
/* close out the file. if the file was opened for write, write out any      */
/* remaining data to the file before closing.                               */
/*                                                                          */
/* THIS IS A SERIAL OPERATION.                                              */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: ~DPvzFile: entering\n", name_only(__FILE__), __LINE__);
#endif

  if (mode == DPvzReadWrite) {
    // closing the file when opened for write must first write the metadata 
    // and table of contents, at least, and possibly the global data section 
    // if it has been modified
    if (write()) {
      fprintf(stderr, "%s: %4d: DPvzFile: unable to write file, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      return;
    }
  }

  if (0 <= fd && close(fd) < 0) {
    // unable to close the file
    fprintf(stderr, "%s: %4d: ~DPvzFile: error occurred when closing '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("close");
  }
  fd = -1;

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: ~DPvzFile: leaving\n", name_only(__FILE__), __LINE__);
#endif
}
#endif

/****************************************************************************/
int64_t DPvzFile::get_global_size()
/****************************************************************************/
/*                                                                          */
/* get the size of the unpadded global section.                             */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/****************************************************************************/
{
  return metadata->global_only;
}

/****************************************************************************/
bool    DPvzFile::get_global_data(void* data)
/****************************************************************************/
/*                                                                          */
/* copy the global data to the buffer. the buffer must be at least as large */
/* as get_global_size() bytes.                                              */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/****************************************************************************/
{
  reset_err();
  if (data != NULL) {
    if (0 < metadata->global_only) {
      memcpy(data, global->data, metadata->global_only);
    }
  } else {
    set_err(DPvzSoft, exit);
  }

exit:
  return failed();
}

/****************************************************************************/
bool    DPvzFile::get_global_data(void* data, ssize_t size, off_t offset)
/****************************************************************************/
/*                                                                          */
/* copy data from the global section to the buffer. data must point to a    */
/* buffer of at least size bytes.                                           */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/****************************************************************************/
{
  reset_err();
  if (data != NULL && 0 < size && 0 <= offset && (size+offset) <= (ssize_t) metadata->global_only) {
    if (0 < size) {
      memcpy(data, global->data+offset, size);
    }
  } else {
    set_err(DPvzSoft, exit);
  }

exit:
  return failed();
}

#if defined(DPVZ_MPI)
/****************************************************************************/
bool    DPvzFile::set_global_data(void* data, int root)
/****************************************************************************/
/*                                                                          */
/* copy data in the buffer to the global section in memory.                 */
/*                                                                          */
/* THIS IS A COLLECTIVE COMMUNICATION ROUTINE.                              */
/* IT REQUIRES ALL RANKS TO PARTICIPATE.                                    */
/* THE COMMUNICATOR comm IS DECLARED IN THE CONSTRUCTOR.                    */
/*                                                                          */
/* root broadcasts its data to all ranks before updating the global data.   */
/*                                                                          */
/* note: the buffer must not be NULL, and the size of the buffer must be at */
/* least as large as get_global_size(). only get_global_size() bytes will   */
/* be copied.                                                               */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_global_data: entering data=%p\n", name_only(__FILE__), __LINE__, data);
#endif

  bool rv = set_global_data(data, metadata->global_only, 0L, root);

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_global_data: leaving\n", name_only(__FILE__), __LINE__);
#endif
  return rv;
}

/****************************************************************************/
bool    DPvzFile::set_global_data(void* data, ssize_t size, off_t offset, int root)
/****************************************************************************/
/*                                                                          */
/* copy data in the buffer to the global section in memory.                 */
/*                                                                          */
/* THIS IS A COLLECTIVE COMMUNICATION ROUTINE.                              */
/* IT REQUIRES ALL RANKS TO PARTICIPATE.                                    */
/* THE COMMUNICATOR comm IS DECLARED IN THE CONSTRUCTOR.                    */
/*                                                                          */
/* root broadcasts its data to all ranks before updating the global data.   */
/*                                                                          */
/* note: the buffer must not be NULL, and the size of the buffer must be    */
/* such that offset + size <= this->get_global_size().                      */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_global_data: entering data=%p size=%ld offset=%ld\n", name_only(__FILE__), __LINE__, data, size, offset);
#endif

  // is there an error in the calling parameters?
  if (data == NULL) {
    // there has to be data to send if you're the sender, or a place to put it if you're the receiver
    fprintf(stderr, "%s: %4d: set_global_data: buffer is NULL, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, check1);
  } else if (offset < 0 || (int64_t) metadata->global_only < (int64_t) offset) {
    // the offset is completely outside of the allocated global data area
    fprintf(stderr, "%s: %4d: set_global_data: the offset (%ld) is outside of the range 0 <= offset < %ld, file '%s'\n", name_only(__FILE__), __LINE__, offset, metadata->global_only, file_name.c_str());
    set_err(DPvzSoft, check1);
  } else if ((ssize_t) metadata->global_only < (size+offset)) {
    // the data would write outside of the assigned global data area
    fprintf(stderr, "%s: %4d: set_global_data: global size (%ld) is less than size (%ld) + offset (%ld), file '%s'\n", name_only(__FILE__), __LINE__, metadata->global_only, size, offset, file_name.c_str());
    set_err(DPvzSoft, check1);
  } else if (mode != DPvzReadWrite && mode != DPvzCreate) {
    fprintf(stderr, "%s: %4d: set_global_data: file mode (0x%x) is not set to ReadWrite or Create, file '%s'\n", name_only(__FILE__), __LINE__, mode, file_name.c_str());
    set_err(DPvzSoft, check1);
  } else if (root < 0 || number_of_ranks <= root) {
    fprintf(stderr, "%s: %4d: set_global_data: root (%d) is not a valid rank, file '%s'\n", name_only(__FILE__), __LINE__, root, file_name.c_str());
    set_err(DPvzSoft, check1);
  }

  // did we encounter an error?
check1:
  MPI_Allreduce(MPI_IN_PLACE, &_err_stat, 1, MPI_INT, MPI_LOR, comm);
  if (failed()) {
    MPI_Allreduce(MPI_IN_PLACE, &_err_line, 1, MPI_INT, MPI_MAX, comm);
    fprintf(stderr, "%s: %4d: set_global_data: error in the call to set_global_data, line=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
    return failed();
  }

  // agree upon the data to store
  MPI_Bcast(data, size, MPI_BYTE, root, comm);

  // everyone writes their local copy to the global data
  global->set_data(metadata, data, size, offset);

  if (rank == root) {
    // the root rank writes the data to the file
    global->write(fd, metadata, file_name);

    if (metadata->write(fd, file_name)) {
      fprintf(stderr, "%s: %4d: set_global_data: rank=%d, unable to write metadata, file '%s'\n", name_only(__FILE__), __LINE__, rank, file_name.c_str());
      set_err(DPvzHard, check2);
    }
  }

check2:
  MPI_Allreduce(MPI_IN_PLACE, &_err_stat, 1, MPI_INT, MPI_LOR, comm);
  if (failed()) {
    MPI_Allreduce(MPI_IN_PLACE, &_err_line, 1, MPI_INT, MPI_MAX, comm);
    fprintf(stderr, "%s: %4d: set_global_data: error in the call to set_global_data, err=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
    return failed();
  }

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_global_data: leaving (%d)\n", name_only(__FILE__), __LINE__, failed());
#endif
  return failed();
}

#else

/****************************************************************************/
bool    DPvzFile::set_global_data(void* data)
/****************************************************************************/
/*                                                                          */
/* copy data in the buffer to the global section in memory.                 */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/* note: the buffer must not be NULL, and the size of the buffer must be at */
/* least as large as get_global_size(). only get_global_size() bytes will   */
/* be copied.                                                               */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_global_data: entering data=%p\n", name_only(__FILE__), __LINE__, data);
#endif

  bool rv = set_global_data(data, metadata->global_only, 0L);

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_global_data: leaving\n", name_only(__FILE__), __LINE__);
#endif
  return rv;
}

/****************************************************************************/
bool    DPvzFile::set_global_data(void* data, ssize_t size, off_t offset)
/****************************************************************************/
/*                                                                          */
/* copy data in the buffer to the global section in memory.                 */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/* note: the buffer must not be NULL, and the size of the buffer must be    */
/* such that offset + size <= this->get_global_size().                      */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_global_data: entering data=%p size=%ld offset=%ld\n", name_only(__FILE__), __LINE__, data, size, offset);
#endif
  reset_err();

  if (data == NULL) {
    // data is missing
    fprintf(stderr, "%s: %4d: set_global_data: buffer is NULL, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, exit);
  } else if ((ssize_t) metadata->global_only < (size+offset)) {
    // data is larger than the available space to store it
    fprintf(stderr, "%s: %4d: set_global_data: global size (%ld) is less than size (%ld) + offset (%ld), file '%s'\n", name_only(__FILE__), __LINE__, metadata->global_only, size, offset, file_name.c_str());
    set_err(DPvzSoft, exit);
  } else if (mode != DPvzReadWrite && mode != DPvzCreate) {
    // file mode doesn't allow changes
    fprintf(stderr, "%s: %4d: set_global_data: file mode (0x%x) is not set to ReadWrite or Create, file '%s'\n", name_only(__FILE__), __LINE__, mode, file_name.c_str());
    set_err(DPvzSoft, exit);
  }

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_global_data: copy the data to the global section\n", name_only(__FILE__), __LINE__);
#endif
  if (global->set_data(metadata, data, size, offset)) {
    fprintf(stderr, "%s: %4d: set_global_data: unable to set global data, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, exit);
  }

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_global_data: write fd=%d metadata=%p file='%s'\n", name_only(__FILE__), __LINE__, fd, metadata, file_name.c_str());
#endif
  if (global->write(fd, metadata, file_name)) {
    fprintf(stderr, "%s: %4d: set_global_data: unable to write global data to the file, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzHard, exit);
  }

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_global_data: write fd=%d file='%s'\n", name_only(__FILE__), __LINE__, fd, file_name.c_str());
#endif
  if (metadata->write(fd, file_name)) {
    fprintf(stderr, "%s: %4d: set_global_data: unable to write metadata, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzHard, exit);
  }

exit:
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_global_data: leaving\n", name_only(__FILE__), __LINE__);
#endif
  return failed();
}

#endif

/****************************************************************************/
void DPvzFile::fprint(FILE* fp)
/****************************************************************************/
/*                                                                          */
/* print the structure.                                                     */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: fprint: rank=%d: entering\n", name_only(__FILE__), __LINE__, rank);
#endif
  fprintf(fp, "DPvzFile\n");
  fprintf(fp, "  file name:        '%s'\n", file_name.c_str());
  fprintf(fp, "  mode:              %s\n", (mode == DPvzReadOnly) ? "read" : ((mode == DPvzReadWrite) ? "write" : ((mode == DPvzCreate) ? "create" : "error") ));
  fprintf(fp, "  fd:                %d\n", fd);
  fprintf(fp, "\n");
  metadata->fprint(fp);
  toc     ->fprint(fp, fd, metadata);
  global  ->fprint(fp, metadata);
  fprintf(fp, "\n");

  // print out the individual time steps
  int time_steps = get_steps();
  fprintf(fp, "  time steps:        %d\n", time_steps);
  if (0 < time_steps) {
    DPvzTocEntry* map = new DPvzTocEntry[time_steps];
    if (get_map(map)) {
      fprintf(stderr, "%s: %4d: fprint: get_map failed, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      return;
    }
    for (int step=0; step < time_steps; step++) {
      int32_t ranks = map[step].ranks;
      DPvzRankToc* step_toc = new DPvzRankToc[ranks];
      if (get_step_toc(map[step], step_toc)) {
	fprintf(stderr, "%s: %4d: fprint: get_step_toc failed, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	return;
      }

      fprintf(fp, "    time step:       %d\n", step);
      for (int rank=0; rank < ranks; rank++) {
	fprintf(fp, "    %d: [%ld, %ld, 0x%016lx, %ld/0x%lx]\n", rank, step_toc[rank].inflated_size, step_toc[rank].deflated_size, step_toc[rank].deflated_crc, step_toc[rank].offset, step_toc[rank].offset);
      }

      fprintf(fp, "\n");

      for (int rank=0; rank < ranks; rank++) {
	fprintf(fp, "    %d: [%ld, %ld, 0x%016lx, %ld/0x%lx]\n", rank, step_toc[rank].inflated_size, step_toc[rank].deflated_size, step_toc[rank].deflated_crc, step_toc[rank].offset, step_toc[rank].offset);

	char* data = new char[step_toc[rank].inflated_size];

	if (get_data(step_toc[rank], data)) {
	  fprintf(stderr, "%s: %4d: fprint: get_step_toc failed, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  return;
	}
	fdump(fp, data, step_toc[rank].inflated_size);

	delete [] data;
      }

      fprintf(fp, "\n");

      delete [] step_toc;
    }

    delete [] map;
  }
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: fprint: rank=%d: leaving\n", name_only(__FILE__), __LINE__, rank);
#endif
}

// read a time step...

/****************************************************************************/
int64_t DPvzFile::get_steps()
/****************************************************************************/
/*                                                                          */
/* get the total number of time steps currently stored in the file. time    */
/* step indexes are contiguous integers from 0 to N-1.                      */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/****************************************************************************/
{
  return metadata->active_entries;
}

/****************************************************************************/
bool DPvzFile::get_map(DPvzTocEntry* map)
/****************************************************************************/
/*                                                                          */
/* get the mapping of time steps to cycle and simulation times. this returns*/
/* the table of contents for the whole file.                                */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/* map contains an array of entries, one per time step, each containing:    */
/*   int32_t ranks;                                                         */
/*   int32_t cycle;                                                         */
/*   double  time;                                                          */
/*   int64_t size;                                                          */
/*   int64_t offset;                                                        */
/*                                                                          */
/* note: the map pointer must point to an array with at least get_steps()   */
/* entries allocated. the size isn't checked within the routine.            */
/*                                                                          */
/* note also that the index of each entry is significant. time step         */
/* retrieval is based on the index of the time step.                        */
/*                                                                          */
/* return true on failure, false otherwise.                                 */
/*                                                                          */
/****************************************************************************/
{
  reset_err();

  DPvzTocEntry mem_L0[DPvzToc::table_len];
  DPvzTocEntry mem_L1[DPvzToc::table_len];

  int mem_L1_idx = -1;
  int mem_L2_idx = -1;

  if (map == NULL) {
    // handle NULL map errors
    fprintf(stderr, "%s: %4d: get_map: map is NULL, it must be an array of size get_steps(), file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, exit);
  }

  for (int idx=0; idx < get_steps(); idx++) {
    DPvzTocIndex toc_idx = toc->get_idx(idx);
    if (toc_idx.table == 0) {
      map[idx] = toc->L0[toc_idx.l0_idx];
    } else if (toc_idx.table == 1) {
      if (mem_L1_idx != toc_idx.l1_idx) {
	// read the L0 table (at toc.L1[l1_idx])
	mem_L1_idx = toc_idx.l1_idx;
	off_t L0_offset = toc->L1[mem_L1_idx].offset;

	// seek to the L0
	if (lseek(fd, L0_offset, SEEK_SET) < 0) {
	  // for some reason we can't do a seek on the file, so bail...
	  fprintf(stderr, "%s: %4d: get_map: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  perror("lseek");
	  set_err(DPvzHard, exit);
	}

	// read the L0
	if (::read(fd, mem_L0, sizeof mem_L0) < (ssize_t) sizeof mem_L0) {
	  // for some reason we can't read the file, so bail...
	  fprintf(stderr, "%s: %4d: get_map: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  perror("read");
	  set_err(DPvzHard, exit);
	}
      }

      // check the CRC of the L0 table
      uint64_t L0_crc64 = crc64(mem_L0, sizeof mem_L0);
      if (toc->L1[mem_L1_idx].toc_crc != L0_crc64) {
	// the CRCs don't match, the file has become corrupted
	fprintf(stderr, "%s: %4d: get_map: CRCs don't match, 0x%lx != 0x%lx, file='%s'\n", name_only(__FILE__), __LINE__, toc->L1[mem_L1_idx].toc_crc, L0_crc64, file_name.c_str());
	set_err(DPvzHard, exit);
      }

      map[idx] = mem_L0[toc_idx.l0_idx];
    } else if (toc_idx.table == 2) {
      if (mem_L2_idx != toc_idx.l2_idx) {
	// read the L1 table (at toc.L2[l2_idx])
	mem_L2_idx = toc_idx.l2_idx;
	mem_L1_idx = -1;
	off_t L1_offset = toc->L2[mem_L2_idx].offset;
	if (lseek(fd, L1_offset, SEEK_SET) < 0) {
	  // for some reason we can't do a seek on the file, so bail...
	  fprintf(stderr, "%s: %4d: get_map: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  perror("lseek");
	  set_err(DPvzHard, exit);
	}

	// read the L1
	if (::read(fd, mem_L1, sizeof mem_L1) < (ssize_t) sizeof mem_L1) {
	  // for some reason we can't read the file, so bail...
	  fprintf(stderr, "%s: %4d: get_map: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  perror("read");
	  set_err(DPvzHard, exit);
	}

	// check the CRC of the L1 table
	uint64_t L1_crc64 = crc64(mem_L1, sizeof mem_L1);
	if (toc->L2[mem_L2_idx].toc_crc != L1_crc64) {
	  // the CRCs don't match, the file has become corrupted
	  fprintf(stderr, "%s: %4d: get_map: CRCs don't match, 0x%lx != 0x%lx, file='%s'\n", name_only(__FILE__), __LINE__, toc->L2[mem_L2_idx].toc_crc, L1_crc64, file_name.c_str());
	  set_err(DPvzHard, exit);
	}
      }

      if (mem_L1_idx != toc_idx.l1_idx) {
	// read the L0 table (at mem_L1[l1_idx])
	mem_L1_idx = toc_idx.l1_idx;
	off_t L0_offset = mem_L1[mem_L1_idx].offset;
	if (lseek(fd, L0_offset, SEEK_SET) < 0) {
	  // for some reason we can't do a seek on the file, so bail...
	  fprintf(stderr, "%s: %4d: get_map: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  perror("lseek");
	  set_err(DPvzHard, exit);
	}

	// read the L0
	if (::read(fd, mem_L0, sizeof mem_L0) < (ssize_t) sizeof mem_L0) {
	  // for some reason we can't read the file, so bail...
	  fprintf(stderr, "%s: %4d: get_map: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	  perror("read");
	  set_err(DPvzHard, exit);
	}

	// check the CRC of the L0 table
	uint64_t L0_crc64 = crc64(mem_L0, sizeof mem_L0);
	if (mem_L1[mem_L1_idx].toc_crc != L0_crc64) {
	  // the CRCs don't match, the file has become corrupted
	  fprintf(stderr, "%s: %4d: get_map: CRCs don't match, 0x%lx != 0x%lx, file='%s'\n", name_only(__FILE__), __LINE__, mem_L1[mem_L1_idx].toc_crc, L0_crc64, file_name.c_str());
	  set_err(DPvzHard, exit);
	}
      }

      map[idx] = mem_L0[toc_idx.l0_idx];
    } else {
      // handle table errors
      fprintf(stderr, "%s: %4d: get_map: bad table number (%d), file='%s'\n", name_only(__FILE__), __LINE__, toc_idx.table, file_name.c_str());
      set_err(DPvzHard, exit);
    }
  }

exit:
  return failed();
}

/****************************************************************************/
DPvzRankToc DPvzFile::get_step_rank(DPvzTocEntry& step, int32_t rank)
/****************************************************************************/
/*                                                                          */
/* get the size and offset of the data block associated with this time step */
/* and rank.                                                                */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/* note: this routine does NOT check the Table of Contents CRC value.       */
/*                                                                          */
/****************************************************************************/
{
  DPvzRankToc rv = DPvzRankToc::unused;

  reset_err();
  if (step.offset == DPvzTocEntry::unused_offset) {
    fprintf(stderr, "%s: %4d: get_sizes: invalid time step, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, exit);
  }

  if (rank < 0 || step.ranks <= rank) {
    fprintf(stderr, "%s: %4d: get_sizes: invalid rank (%d), file='%s'\n", name_only(__FILE__), __LINE__, rank, file_name.c_str());
    set_err(DPvzSoft, exit);
  }

  if (lseek(fd, step.offset + rank * sizeof(DPvzRankToc), SEEK_SET) < 0) {
    // for some reason we can't do a seek on the file, so bail...
    fprintf(stderr, "%s: %4d: get_sizes: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    set_err(DPvzHard, exit);
  }

  // read the time step table of contents
  if (::read(fd, &rv, sizeof(DPvzRankToc)) < (ssize_t) sizeof(DPvzRankToc)) {
    // for some reason we can't read the file, so bail...
    fprintf(stderr, "%s: %4d: get_sizes: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("read");
    set_err(DPvzHard, exit);
  }

exit:
  return rv;
}

/****************************************************************************/
bool DPvzFile::get_step_toc(DPvzTocEntry& step, DPvzRankToc* step_toc)
/****************************************************************************/
/*                                                                          */
/* get the size and offset associated with all ranks at this time step.     */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/* note: "step_toc" must be an array of at least step.ranks elements.       */
/*                                                                          */
/* return true on failure, false on success.                                */
/*                                                                          */
/****************************************************************************/
{
  reset_err();

  ssize_t  step_toc_size = 0;
  uint64_t step_toc_crc  = 0;

  if (step.offset == DPvzTocEntry::unused_offset) {
    fprintf(stderr, "%s: %4d: get_sizes: invalid time step, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, exit);
  }

  if (step_toc == NULL) {
    fprintf(stderr, "%s: %4d: get_sizes: array size cannot be NULL, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, exit);
  }

  if (lseek(fd, step.offset, SEEK_SET) < 0) {
    // for some reason we can't do a seek on the file, so bail...
    fprintf(stderr, "%s: %4d: get_sizes: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    set_err(DPvzHard, exit);
  }

  // read the time step table of contents
  step_toc_size = (ssize_t) (step.ranks * sizeof(DPvzRankToc));
  if (::read(fd, step_toc, step_toc_size) < step_toc_size) {
    // for some reason we can't read the file, so bail...
    fprintf(stderr, "%s: %4d: get_sizes: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("read");
    set_err(DPvzHard, exit);
  }

  // check the table of contents crc value
  step_toc_crc = crc64(step_toc, step_toc_size);
  if (step_toc_crc != step.toc_crc) {
    fprintf(stderr, "%s: %4d: get_step_toc: crc doesn't match, 0x%016lx != 0x%016lx, file='%s'\n", name_only(__FILE__), __LINE__, step_toc_crc, step.toc_crc, file_name.c_str());
    set_err(DPvzHard, exit);
  }

exit:
  return failed();
}

/****************************************************************************/
bool DPvzFile::get_data (DPvzRankToc& step_rank, void* inflated_data)
/****************************************************************************/
/*                                                                          */
/* get a copy of the data block associated with this time step and rank.    */
/* inflated_data must point to a buffer with at least                       */
/* step_rank.inflated_size bytes.                                           */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/* return true if the call fails, false otherwise.                          */
/*                                                                          */
/****************************************************************************/
{
  char*    deflated_data = NULL;
  uint64_t deflated_crc  = 0;
  int64_t  inflated_size = 0;

  reset_err();

  if (step_rank == DPvzRankToc::unused) {
    fprintf(stderr, "%s: %4d: get_data: invalid time step entry, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, exit);
  }

  if (inflated_data == NULL) {
    fprintf(stderr, "%s: %4d: get_data: array size cannot be NULL, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, exit);
  }

  // seek to the deflated data
  if (lseek(fd, step_rank.offset, SEEK_SET) < 0) {
    // for some reason we can't do a seek on the file, so bail...
    fprintf(stderr, "%s: %4d: get_data: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    set_err(DPvzHard, exit);
  }

  // read the deflated data
  deflated_data = new char[step_rank.deflated_size + 1024];
  if (::read(fd, deflated_data, step_rank.deflated_size) < (ssize_t) step_rank.deflated_size) {
    // for some reason we can't read the file, so bail...
    fprintf(stderr, "%s: %4d: get_data: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("read");
    set_err(DPvzHard, exit);
  }

  // check the crc on the deflated data
  deflated_crc = crc64(deflated_data, step_rank.deflated_size);
  if (deflated_crc != step_rank.deflated_crc) {
    fprintf(stderr, "%s: %4d: get_data: crc doesn't match, 0x%016lx != 0x%016lx, file='%s'\n", name_only(__FILE__), __LINE__, deflated_crc, step_rank.deflated_crc, file_name.c_str());
    set_err(DPvzHard, exit);
  }

  // inflate (decompress) the data
  memset(inflated_data, '\0', step_rank.inflated_size);
  inflated_size = inflate_buf(inflated_data, step_rank.inflated_size, deflated_data, step_rank.deflated_size);
  if (inflated_size != step_rank.inflated_size) {
    fprintf(stderr, "%s: %4d: get_data: inflated size (%ld) doesn't match expected size (%ld), file='%s'\n", name_only(__FILE__), __LINE__, inflated_size, step_rank.inflated_size, file_name.c_str());
    set_err(DPvzHard, exit);
  }

exit:
  if (deflated_data != NULL) {
    delete [] deflated_data;
  }

  return failed();
}

#if defined(DPVZ_MPI)

/****************************************************************************/
bool DPvzFile::write(int64_t cycle, double time, std::string buf, int32_t root)
/****************************************************************************/
/*                                                                          */
/* write a time step.                                                       */
/*                                                                          */
/* THIS IS A COLLECTIVE COMMUNICATION ROUTINE.                              */
/* IT REQUIRES ALL RANKS TO PARTICIPATE.                                    */
/*                                                                          */
/* only values for cycle and time from rank "root" are honored. values from */
/* other ranks are overwritten.                                             */
/*                                                                          */
/* note: this needs to be a collective operation to get the rank id, total  */
/* number of ranks, size of each block, and total data size properly        */
/* allocated in the file.                                                   */
/*                                                                          */
/* return true if the call fails, false otherwise.                          */
/*                                                                          */
/****************************************************************************/
{
  // translate this to a call to write
  return write(cycle, time, (const void*) buf.c_str(), buf.size(), root);
}

/****************************************************************************/
bool DPvzFile::write(int64_t cycle, double time, const void* buf, ssize_t size, int32_t root)
/****************************************************************************/
/*                                                                          */
/* write a time step.                                                       */
/*                                                                          */
/* THIS IS A COLLECTIVE COMMUNICATION ROUTINE.                              */
/* IT REQUIRES ALL RANKS TO PARTICIPATE.                                    */
/*                                                                          */
/* only values for cycle and time from rank "root" are honored. values from */
/* other ranks are overwritten.                                             */
/*                                                                          */
/* note: this needs to be a collective operation to get the rank id, total  */
/* number of ranks, size of each block, and total data size properly        */
/* allocated in the file.                                                   */
/*                                                                          */
/* return true if the call fails, false otherwise.                          */
/*                                                                          */
/****************************************************************************/
{
  reset_err();

  if (buf == NULL && 0 < size) {
    fprintf(stderr, "%s: %4d: write: buffer cannot be NULL with size greater than zero, buf=%p, size=%ld, file='%s'\n", name_only(__FILE__), __LINE__, buf, size, file_name.c_str());
    set_err(DPvzSoft, check1);
  }

  if (mode == DPvzReadOnly) {
    fprintf(stderr, "%s: %4d: write: attempt to write with mode == Read Only, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, check1);
  }

  if (cycle != (int64_t) ((int32_t) cycle)) {
    fprintf(stderr, "%s: %4d: write: attempt to write a cycle larger than 2,147,483,647 (0x7FFFFFFF), file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, check1);
  }

check1:
  MPI_Allreduce(MPI_IN_PLACE, &_err_stat, 1, MPI_INT, MPI_LOR, comm);
  if (failed()) {
    MPI_Allreduce(MPI_IN_PLACE, &_err_line, 1, MPI_INT, MPI_MAX, comm);
    fprintf(stderr, "%s: %4d: write: error writing file, err=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
    return true;
  }

#if defined(DPVZ_MPI)
  // make sure everyone agrees on the values for cycle and time
  MPI_Bcast(&cycle, 1, MPI_INT64_T, root, comm);
  MPI_Bcast(&time,  1, MPI_DOUBLE,  root, comm);
#endif

  int64_t L0_offset = metadata->toc_offset;
  int64_t L1_offset = L0_offset + sizeof toc->L0;

  bool write_L0 = false;
  bool write_L1 = false;
  bool write_L2 = false;

  int64_t end_of_file = pad(metadata->total_file_size, metadata->page_size);

  DPvzTocEntry mem_L0[DPvzToc::table_len];
  DPvzTocEntry mem_L1[DPvzToc::table_len];
  DPvzTocEntry mem_L2[DPvzToc::table_len];

  // allocate a new L0 and L1, if needed
  // when this section is done, the relevant ToC will be in mem_L[0-2] and the offsets will be in L[0-2]_offset
  DPvzTocIndex toc_idx = toc->get_idx(metadata->active_entries);
  if (toc_idx.table == 0) {
    // we're in L0
    memcpy(mem_L0, toc->L0, sizeof toc->L0);
    write_L0 = true;
  } else if (toc_idx.table == 1) {
    // we're in L1
    memcpy(mem_L1, toc->L1, sizeof toc->L1);
    write_L0 = true;
    write_L1 = true;
    if (toc_idx.l0_idx == 0) {
      // we need to allocate a new L0 entry in the L1
      L0_offset = pad(end_of_file, metadata->page_size);
      end_of_file = pad(end_of_file + sizeof toc->L0, metadata->page_size);

      // set the L1 entry to the new L0
      mem_L1[toc_idx.l1_idx].ranks   = number_of_ranks;
      mem_L1[toc_idx.l1_idx].cycle   = (int32_t) cycle;
      mem_L1[toc_idx.l1_idx].time    = time;
      mem_L1[toc_idx.l1_idx].toc_crc = 0L;
      mem_L1[toc_idx.l1_idx].offset  = L0_offset;

      for (int i=0; i < DPvzToc::table_len; i++) {
	mem_L0[i] = DPvzTocEntry::unused;
      }
    } else {
      // we need to read the L0
      L0_offset = toc->L1[toc_idx.l1_idx].offset;

      // seek to the L0 on disk
      if (lseek(fd, L0_offset, SEEK_SET) < 0) {
	// for some reason we can't do a seek on the file, so bail...
	fprintf(stderr, "%s: %4d: write: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("lseek");
	set_err(DPvzHard, check2);
      }

      // read the L0
      if (::read(fd, mem_L0, sizeof mem_L0) < (ssize_t) sizeof mem_L0) {
	// for some reason we can't read the file, so bail...
	fprintf(stderr, "%s: %4d: write: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("read");
	set_err(DPvzHard, check2);
      }
    }
  } else if (toc_idx.table == 2) {
    // we're in L2
    memcpy(mem_L2, toc->L2, sizeof toc->L2);
    write_L0 = true;
    write_L1 = true;
    write_L2 = true;
    if (toc_idx.l1_idx == 0 && toc_idx.l0_idx == 0) {
      // we need to allocate a new L1 entry in the L2
      L1_offset = pad(end_of_file, metadata->page_size);
      end_of_file = pad(end_of_file + sizeof toc->L1, metadata->page_size);

      // set the L2 entry to the new L1
      mem_L2[toc_idx.l2_idx].ranks   = number_of_ranks;
      mem_L2[toc_idx.l2_idx].cycle   = (int32_t) cycle;
      mem_L2[toc_idx.l2_idx].time    = time;
      mem_L2[toc_idx.l2_idx].toc_crc = 0L;
      mem_L2[toc_idx.l2_idx].offset  = L1_offset;

      for (int i=0; i < DPvzToc::table_len; i++) {
	mem_L1[i] = DPvzTocEntry::unused;
      }
    } else {
      // we need to read the L1
      L1_offset = toc->L2[toc_idx.l2_idx].offset;

      // seek to the L1 on disk
      if (lseek(fd, L1_offset, SEEK_SET) < 0) {
	// for some reason we can't do a seek on the file, so bail...
	fprintf(stderr, "%s: %4d: write: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("lseek");
	set_err(DPvzHard, check2);
      }

      // read the L1
      if (::read(fd, mem_L1, sizeof mem_L1) < (ssize_t) sizeof mem_L1) {
	// for some reason we can't read the file, so bail...
	fprintf(stderr, "%s: %4d: write: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("read");
	set_err(DPvzHard, check2);
      }
    }

    if (toc_idx.l0_idx == 0) {
      // we need to allocate a new L0 entry
      L0_offset = pad(end_of_file, metadata->page_size);
      end_of_file = pad(end_of_file + sizeof toc->L0, metadata->page_size);

      // set the L1 entry to the new L0
      mem_L1[toc_idx.l1_idx].ranks   = number_of_ranks;
      mem_L1[toc_idx.l1_idx].cycle   = (int32_t) cycle;
      mem_L1[toc_idx.l1_idx].time    = time;
      mem_L1[toc_idx.l1_idx].toc_crc = 0L;
      mem_L1[toc_idx.l1_idx].offset  = L0_offset;

      for (int i=0; i < DPvzToc::table_len; i++) {
	mem_L0[i] = DPvzTocEntry::unused;
      }
    } else {
      // we need to read the L0
      L0_offset = toc->L1[toc_idx.l1_idx].offset;

      // seek to the L0 on disk
      if (lseek(fd, L0_offset, SEEK_SET) < 0) {
	// for some reason we can't do a seek on the file, so bail...
	fprintf(stderr, "%s: %4d: write: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("lseek");
	set_err(DPvzHard, check2);
      }

      // read the L0
      if (::read(fd, mem_L0, sizeof mem_L0) < (ssize_t) sizeof mem_L0) {
	// for some reason we can't read the file, so bail...
	fprintf(stderr, "%s: %4d: write: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("read");
	set_err(DPvzHard, check2);
      }
    }
  } else {
    // for some reason have a bad table index...
    fprintf(stderr, "%s: %4d: write: bad table index (%d), active entries=%ld, file='%s'\n", name_only(__FILE__), __LINE__, toc_idx.table, metadata->active_entries, file_name.c_str());
    set_err(DPvzHard, check2);
  }

check2:
  MPI_Allreduce(MPI_IN_PLACE, &_err_stat, 1, MPI_INT, MPI_LOR, comm);
  if (failed()) {
    MPI_Allreduce(MPI_IN_PLACE, &_err_line, 1, MPI_INT, MPI_MAX, comm);
    fprintf(stderr, "%s: %4d: write: error writing file, err=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
    return true;
  }

  // set the L0 entry
  mem_L0[toc_idx.l0_idx].ranks   = number_of_ranks;
  mem_L0[toc_idx.l0_idx].cycle   = (int32_t) cycle;
  mem_L0[toc_idx.l0_idx].time    = time;
  mem_L0[toc_idx.l0_idx].toc_crc = 0L;
  mem_L0[toc_idx.l0_idx].offset  = end_of_file;

  // L0, L1, and L2 are now in memory, but not on disk, and the CRCs are not correct
  // NO CHANGES HAVE BEEN WRITTEN TO THE FILE OR TO THE METADATA OR TOC STRUCTURE
  int64_t time_step_start        = pad(end_of_file, metadata->page_size);
  int64_t time_step_toc_start    = time_step_start;
  int64_t time_step_toc_size     = sizeof(DPvzRankToc[number_of_ranks]);
  int64_t time_step_data_start   = time_step_toc_start + time_step_toc_size;

  // compress (deflate) the local data
  int         inflated_size = size;
  const void* inflated_data = buf;

  int   deflated_max  = pad(inflated_size + metadata->page_size, metadata->page_size);
  void* deflated_data = malloc(deflated_max);
  int   deflated_size = deflate_buf(deflated_data, deflated_max, inflated_data, inflated_size, metadata->compression_algorithm);

  // compute the local crc on the deflated data
  uint64_t deflated_crc = crc64(deflated_data, deflated_size);

  // MPI_Alltoall to get the inflated and deflated sizes, and crc, for each rank
  DPvzRankToc* sendbuf = new DPvzRankToc[number_of_ranks];
  DPvzRankToc* recvbuf = new DPvzRankToc[number_of_ranks];
  {
    DPvzRankToc b = DPvzRankToc( inflated_size, deflated_size, deflated_crc, 0L );
    for (int i=0; i < number_of_ranks; i++) {
      sendbuf[i] = b;
    }
  }

  // make the call...
  int count = sizeof(DPvzRankToc) / sizeof(int64_t);
  MPI_Alltoall((const void *) sendbuf, count, MPI_INT64_T, (void *) recvbuf, count, MPI_INT64_T, comm);

  // compute the time step table of contents (size, offset, and CRC) and total size
  DPvzRankToc* time_step_toc = recvbuf;
  time_step_toc[0].offset = time_step_data_start;
  for (int i=1; i < number_of_ranks; i++) {
    // find the data offset, rounded up to the next page boundary
    time_step_toc[i].offset = pad(time_step_toc[i-1].offset + time_step_toc[i-1].deflated_size, metadata->page_size);
  }

  // find the new end-of-file
  end_of_file = pad(time_step_toc[number_of_ranks-1].offset + time_step_toc[number_of_ranks-1].deflated_size, metadata->page_size); 

  // compute the CRCs for the ToC
  if (write_L0) {
    mem_L0[toc_idx.l0_idx].toc_crc = crc64(time_step_toc, time_step_toc_size);
  }

  if (write_L1) {
    mem_L1[toc_idx.l1_idx].toc_crc = crc64(mem_L0, sizeof toc->L0);
  }

  if (write_L2) {
    mem_L2[toc_idx.l2_idx].toc_crc = crc64(mem_L1, sizeof toc->L1);
  }

  // we are now ready to update the metadata and toc structures in memory
  metadata->active_entries += 1;
  metadata->total_file_size = end_of_file;
  if (write_L0) {
    toc->L0[toc_idx.l0_idx] = mem_L0[toc_idx.l0_idx];
  }

  if (write_L1) {
    toc->L1[toc_idx.l1_idx] = mem_L1[toc_idx.l1_idx];
  }

  if (write_L2) {
    toc->L2[toc_idx.l2_idx] = mem_L2[toc_idx.l2_idx];
  }

  // write the metadata and ToC to the file
  if (rank == root) {
    // seek to the new last page in the file and write a page of zeros
    int64_t last_page_of_file = end_of_file - metadata->page_size;
    void* zero_page = malloc(metadata->page_size);
    memset(zero_page, '\0', metadata->page_size);

    // seek to the start of the last page
    if (lseek(fd, last_page_of_file, SEEK_SET) < 0) {
      // for some reason we can't do a seek on the file, so bail...
      fprintf(stderr, "%s: %4d: write: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      perror("lseek");
      free(zero_page);
      set_err(DPvzHard, check3);
    }

    // write the last page
    // (this works because we have not yet written the data to the file)
    if (::write(fd, zero_page, metadata->page_size) < (ssize_t) metadata->page_size) {
      // for some reason we can't write the file, so bail...
      fprintf(stderr, "%s: %4d: write: unable to write, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      perror("write");
      free(zero_page);
      set_err(DPvzHard, check3);
    }

    free(zero_page);

    // write the ToC and metadata back to the file
    write();

    // write the time step ToC data...
    // seek to the start of the time step toc
    if (lseek(fd, time_step_toc_start, SEEK_SET) < 0) {
      // for some reason we can't do a seek on the file, so bail...
      fprintf(stderr, "%s: %4d: write: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      perror("lseek");
      set_err(DPvzHard, check3);
    }

    // write the time step toc
    if (::write(fd, time_step_toc, time_step_toc_size) < time_step_toc_size) {
      // for some reason we can't write the file, so bail...
      fprintf(stderr, "%s: %4d: write: unable to write, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      perror("write");
      set_err(DPvzHard, check3);
    }

    // flush the data back to disk
    fsync(fd);
  }

check3:
  // rank 0 is done writing the metadata and ToC
  MPI_Allreduce(MPI_IN_PLACE, &_err_stat, 1, MPI_INT, MPI_LOR, comm);
  if (failed()) {
    delete [] recvbuf;
    delete [] sendbuf;
    free(deflated_data);

    MPI_Allreduce(MPI_IN_PLACE, &_err_line, 1, MPI_INT, MPI_MAX, comm);
    fprintf(stderr, "%s: %4d: write: error writing file, err=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
    return true;
  }

  // each rank individually writes their data to the file...
  if (0 < size) {
    // seek to the time step data for this rank
    if (lseek(fd, time_step_toc[rank].offset, SEEK_SET) < 0) {
      // for some reason we can't do a seek on the file, so bail...
      fprintf(stderr, "%s: %4d: write: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      perror("lseek");
      set_err(DPvzHard, check4);
    }

    // write the time step toc
    if (::write(fd, deflated_data, deflated_size) < deflated_size) {
      // for some reason we can't write the file, so bail...
      fprintf(stderr, "%s: %4d: write: unable to write, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      perror("write");
      set_err(DPvzHard, check4);
    }

    // flush all of the data back to disk
    if (fsync(fd) < 0) {
      // for some reason we can't flush data to the file, so bail...
      fprintf(stderr, "%s: %4d: write: unable to flush data, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      perror("fsync");
      set_err(DPvzHard, check4);
    }
  }

check4:
  delete [] recvbuf;
  delete [] sendbuf;
  free(deflated_data);

  MPI_Allreduce(MPI_IN_PLACE, &_err_stat, 1, MPI_INT, MPI_LOR, comm);
  if (failed()) {
    MPI_Allreduce(MPI_IN_PLACE, &_err_line, 1, MPI_INT, MPI_MAX, comm);
    fprintf(stderr, "%s: %4d: write: error writing file, err=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
    return true;
  }

  return failed();
}

#else

/****************************************************************************/
bool DPvzFile::write(int64_t cycle, double time, std::string buf)
/****************************************************************************/
/*                                                                          */
/* write a time step.                                                       */
/*                                                                          */
/* THIS IS A LOCAL VERSION OF THE OPERATION. IT IS EXECUTED SERIALLY.       */
/*                                                                          */
/* return true if the call fails, false otherwise.                          */
/*                                                                          */
/****************************************************************************/
{
  // translate this to a call to write
  return write(cycle, time, (const void*) buf.c_str(), buf.size());
}

/****************************************************************************/
bool DPvzFile::write(int64_t cycle, double time, const void* buf, ssize_t size)
/****************************************************************************/
/*                                                                          */
/* write a time step.                                                       */
/*                                                                          */
/* THIS IS A LOCAL VERSION OF THE OPERATION. IT IS EXECUTED SERIALLY.       */
/*                                                                          */
/* return true if the call fails, false otherwise.                          */
/*                                                                          */
/****************************************************************************/
{
  DPvzRankToc* sendbuf = NULL;
  DPvzRankToc* recvbuf = NULL;
  void* zero_page      = NULL;

  DPvzRankToc* time_step_toc     = NULL;
  int64_t time_step_start        = 0;
  int64_t time_step_toc_start    = 0;
  int64_t time_step_toc_size     = 0;
  int64_t time_step_data_start   = 0;

  void*    deflated_data = NULL;
  int      deflated_max  = 0;
  uint64_t deflated_crc  = 0;
  int      deflated_size = 0;

  int   inflated_size       = 0;
  const void* inflated_data = NULL;

  reset_err();

  // check the arguments
  if (buf == NULL && 0 < size) {
    fprintf(stderr, "%s: %4d: write: buffer cannot be NULL with size greater than zero, buf=%p, size=%ld, file='%s'\n", name_only(__FILE__), __LINE__, buf, size, file_name.c_str());
    set_err(DPvzSoft, check1);
  }

  // check the mode
  if (! (mode == DPvzReadWrite || mode == DPvzCreate)) {
    fprintf(stderr, "%s: %4d: write: attempt to write with mode == Read Only, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, check1);
  }

  if (cycle != (int64_t) ((int32_t) cycle)) {
    fprintf(stderr, "%s: %4d: write: attempt to write a cycle larger than 2,147,483,647 (0x7FFFFFFF), file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, check1);
  }

check1:
  if (failed()) {
    return failed();
  }

  // allocate the new time step
  int64_t L0_offset = metadata->toc_offset;
  int64_t L1_offset = L0_offset + sizeof toc->L0;

  bool write_L0 = false;
  bool write_L1 = false;
  bool write_L2 = false;

  int64_t end_of_file = pad(metadata->total_file_size, metadata->page_size);

  DPvzTocEntry mem_L0[DPvzToc::table_len];
  DPvzTocEntry mem_L1[DPvzToc::table_len];
  DPvzTocEntry mem_L2[DPvzToc::table_len];

  // allocate a new L0 and L1, if needed
  // when this section is done, the relevant ToC will be in mem_L[0-2] and the offsets will be in L[0-2]_offset
  DPvzTocIndex toc_idx = toc->get_idx(metadata->active_entries);
  if (toc_idx.table == 0) {
    // we're in L0
    memcpy(mem_L0, toc->L0, sizeof toc->L0);
    write_L0 = true;
  } else if (toc_idx.table == 1) {
    // we're in L1
    memcpy(mem_L1, toc->L1, sizeof toc->L1);
    write_L0 = true;
    write_L1 = true;
    if (toc_idx.l0_idx == 0) {
      // we need to allocate a new L0 entry in the L1
      L0_offset = pad(end_of_file, metadata->page_size);
      end_of_file = pad(end_of_file + sizeof toc->L0, metadata->page_size);

      // set the L1 entry to the new L0
      mem_L1[toc_idx.l1_idx].ranks   = number_of_ranks;
      mem_L1[toc_idx.l1_idx].cycle   = (int32_t) cycle;
      mem_L1[toc_idx.l1_idx].time    = time;
      mem_L1[toc_idx.l1_idx].toc_crc = 0L;
      mem_L1[toc_idx.l1_idx].offset  = L0_offset;

      for (int i=0; i < DPvzToc::table_len; i++) {
	mem_L0[i] = DPvzTocEntry::unused;
      }
    } else {
      // we need to read the L0
      L0_offset = toc->L1[toc_idx.l1_idx].offset;

      // seek to the L0 on disk
      if (lseek(fd, L0_offset, SEEK_SET) < 0) {
	// for some reason we can't do a seek on the file, so bail...
	fprintf(stderr, "%s: %4d: write: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("lseek");
	set_err(DPvzHard, exit);
      }

      // read the L0
      if (::read(fd, mem_L0, sizeof mem_L0) < (ssize_t) sizeof mem_L0) {
	// for some reason we can't read the file, so bail...
	fprintf(stderr, "%s: %4d: write: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("read");
	set_err(DPvzHard, exit);
      }
    }
  } else if (toc_idx.table == 2) {
    // we're in L2
    memcpy(mem_L2, toc->L2, sizeof toc->L2);
    write_L0 = true;
    write_L1 = true;
    write_L2 = true;
    if (toc_idx.l1_idx == 0 && toc_idx.l0_idx == 0) {
      // we need to allocate a new L1 entry in the L2
      L1_offset = pad(end_of_file, metadata->page_size);
      end_of_file = pad(end_of_file + sizeof toc->L1, metadata->page_size);

      // set the L2 entry to the new L1
      mem_L2[toc_idx.l2_idx].ranks   = number_of_ranks;
      mem_L2[toc_idx.l2_idx].cycle   = (int32_t) cycle;
      mem_L2[toc_idx.l2_idx].time    = time;
      mem_L2[toc_idx.l2_idx].toc_crc = 0L;
      mem_L2[toc_idx.l2_idx].offset  = L1_offset;

      for (int i=0; i < DPvzToc::table_len; i++) {
	mem_L1[i] = DPvzTocEntry::unused;
      }
    } else {
      // we need to read the L1
      L1_offset = toc->L2[toc_idx.l2_idx].offset;

      // seek to the L1 on disk
      if (lseek(fd, L1_offset, SEEK_SET) < 0) {
	// for some reason we can't do a seek on the file, so bail...
	fprintf(stderr, "%s: %4d: write: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("lseek");
	set_err(DPvzHard, exit);
      }

      // read the L1
      if (::read(fd, mem_L1, sizeof mem_L1) < (ssize_t) sizeof mem_L1) {
	// for some reason we can't read the file, so bail...
	fprintf(stderr, "%s: %4d: write: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("read");
	set_err(DPvzHard, exit);
      }
    }

    if (toc_idx.l0_idx == 0) {
      // we need to allocate a new L0 entry
      L0_offset = pad(end_of_file, metadata->page_size);
      end_of_file = pad(end_of_file + sizeof toc->L0, metadata->page_size);

      // set the L1 entry to the new L0
      mem_L1[toc_idx.l1_idx].ranks   = number_of_ranks;
      mem_L1[toc_idx.l1_idx].cycle   = (int32_t) cycle;
      mem_L1[toc_idx.l1_idx].time    = time;
      mem_L1[toc_idx.l1_idx].toc_crc = 0L;
      mem_L1[toc_idx.l1_idx].offset  = L0_offset;

      for (int i=0; i < DPvzToc::table_len; i++) {
	mem_L0[i] = DPvzTocEntry::unused;
      }
    } else {
      // we need to read the L0
      L0_offset = toc->L1[toc_idx.l1_idx].offset;

      // seek to the L0 on disk
      if (lseek(fd, L0_offset, SEEK_SET) < 0) {
	// for some reason we can't do a seek on the file, so bail...
	fprintf(stderr, "%s: %4d: write: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("lseek");
	set_err(DPvzHard, exit);
      }

      // read the L0
      if (::read(fd, mem_L0, sizeof mem_L0) < (ssize_t) sizeof mem_L0) {
	// for some reason we can't read the file, so bail...
	fprintf(stderr, "%s: %4d: write: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("read");
	set_err(DPvzHard, exit);
      }
    }
  } else {
    // for some reason have a bad table index...
    fprintf(stderr, "%s: %4d: write: bad table index (%d), active entries=%ld, file='%s'\n", name_only(__FILE__), __LINE__, toc_idx.table, metadata->active_entries, file_name.c_str());
    set_err(DPvzHard, exit);
  }

  // set the L0 entry
  mem_L0[toc_idx.l0_idx].ranks   = number_of_ranks;
  mem_L0[toc_idx.l0_idx].cycle   = (int32_t) cycle;
  mem_L0[toc_idx.l0_idx].time    = time;
  mem_L0[toc_idx.l0_idx].toc_crc = 0L;
  mem_L0[toc_idx.l0_idx].offset  = end_of_file;

  // L0, L1, and L2 are now in memory, but not on disk, and the CRCs are not correct
  // NO CHANGES HAVE BEEN WRITTEN TO THE FILE OR TO THE METADATA OR TOC STRUCTURE
  time_step_start      = pad(end_of_file, metadata->page_size);
  time_step_toc_start  = time_step_start;
  time_step_toc_size   = sizeof(DPvzRankToc[number_of_ranks]);
  time_step_data_start = time_step_toc_start + time_step_toc_size;

  // compress (deflate) the local data
  inflated_size = size;
  inflated_data = buf;

  deflated_max  = 0;
  deflated_data = NULL;
  deflated_size = 0;
  if (0 < size) {
    deflated_max  = pad(inflated_size + metadata->page_size, metadata->page_size);
    deflated_data = malloc(deflated_max);
    deflated_size = deflate_buf(deflated_data, deflated_max, inflated_data, inflated_size, metadata->compression_algorithm);
  }

  // compute the local crc on the deflated data
  deflated_crc = crc64(deflated_data, deflated_size);

  // MPI_Alltoall to get the inflated and deflated sizes, and crc, for each rank
  sendbuf = new DPvzRankToc[number_of_ranks];
  recvbuf = new DPvzRankToc[number_of_ranks];
  {
    DPvzRankToc b = DPvzRankToc( inflated_size, deflated_size, deflated_crc, 0L );
    for (int i=0; i < number_of_ranks; i++) {
      sendbuf[i] = b;
    }
  }

  // make the call...
  // int count = sizeof(DPvzRankToc) / sizeof(int64_t);
  for (int i=0; i < number_of_ranks; i++) {
    recvbuf[i] = sendbuf[i];
  }

  // compute the time step table of contents (size, offset, and CRC) and total size
  time_step_toc = recvbuf;
  time_step_toc[0].offset = time_step_data_start;
  for (int i=1; i < number_of_ranks; i++) {
    // find the data offset, rounded up to the next page boundary
    time_step_toc[i].offset = pad(time_step_toc[i-1].offset + time_step_toc[i-1].deflated_size, metadata->page_size);
  }

  // find the new end-of-file
  end_of_file = pad(time_step_toc[number_of_ranks-1].offset + time_step_toc[number_of_ranks-1].deflated_size, metadata->page_size); 

  // compute the CRCs for the ToC
  if (write_L0) {
    mem_L0[toc_idx.l0_idx].toc_crc = crc64(time_step_toc, time_step_toc_size);
  }

  if (write_L1) {
    mem_L1[toc_idx.l1_idx].toc_crc = crc64(mem_L0, sizeof toc->L0);
  }

  if (write_L2) {
    mem_L2[toc_idx.l2_idx].toc_crc = crc64(mem_L1, sizeof toc->L1);
  }

  // we are now ready to update the metadata and toc structures in memory
  metadata->active_entries += 1;
  metadata->total_file_size = end_of_file;
  if (write_L0) {
    toc->L0[toc_idx.l0_idx] = mem_L0[toc_idx.l0_idx];
  }

  if (write_L1) {
    toc->L1[toc_idx.l1_idx] = mem_L1[toc_idx.l1_idx];
  }

  if (write_L2) {
    toc->L2[toc_idx.l2_idx] = mem_L2[toc_idx.l2_idx];
  }

  // write the metadata and ToC to the file

  {	// DMP TODO
  // allocate a page of zeros
  int64_t last_page_of_file = end_of_file - metadata->page_size;
  zero_page = malloc(metadata->page_size);
  memset(zero_page, '\0', metadata->page_size);

  // seek to the start of the last page
  if (lseek(fd, last_page_of_file, SEEK_SET) < 0) {
    // for some reason we can't do a seek on the file, so bail...
    fprintf(stderr, "%s: %4d: write: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    set_err(DPvzHard, exit);
  }

  // write a page of zeros to the last page of the file
  // (this works because we have not yet written the data to the file)
  if (::write(fd, zero_page, metadata->page_size) < (ssize_t) metadata->page_size) {
    // for some reason we can't write the file, so bail...
    fprintf(stderr, "%s: %4d: write: unable to write, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("write");
    set_err(DPvzHard, exit);
  }

  free(zero_page);
  }

  // write the ToC and metadata back to the file
  write();

  // write the time step ToC data...
  // seek to the start of the time step toc
  if (lseek(fd, time_step_toc_start, SEEK_SET) < 0) {
    // for some reason we can't do a seek on the file, so bail...
    fprintf(stderr, "%s: %4d: write: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    set_err(DPvzHard, exit);
  }

  // write the time step toc
  if (::write(fd, time_step_toc, time_step_toc_size) < time_step_toc_size) {
    // for some reason we can't write the file, so bail...
    fprintf(stderr, "%s: %4d: write: unable to write, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("write");
    set_err(DPvzHard, exit);
  }

  // flush the data back to disk
  if (fsync(fd) < 0) {
    // for some reason we can't flush data to the file, so bail...
    fprintf(stderr, "%s: %4d: write: unable to flush data, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("fsync");
    set_err(DPvzHard, exit);
  }

  // write the time step data to the file
  // seek to the time step data for this rank
  if (lseek(fd, time_step_toc[rank].offset, SEEK_SET) < 0) {
    // for some reason we can't do a seek on the file, so bail...
    fprintf(stderr, "%s: %4d: write: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    set_err(DPvzHard, exit);
  }

  // write the time step toc
  if (::write(fd, deflated_data, deflated_size) < deflated_size) {
    // for some reason we can't write the file, so bail...
    fprintf(stderr, "%s: %4d: write: unable to write, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("write");
    set_err(DPvzHard, exit);
  }

  // flush all of the data back to disk
  if (fsync(fd) < 0) {
    // for some reason we can't flush data to the file, so bail...
    fprintf(stderr, "%s: %4d: write: unable to flush data, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("fsync");
    set_err(DPvzHard, exit);
  }

exit:
  if (recvbuf       != NULL) delete [] recvbuf;
  if (sendbuf       != NULL) delete [] sendbuf;
  if (deflated_data != NULL) free(deflated_data);

  return failed();
}
#endif

/****************************************************************************/
#if defined(DPVZ_MPI)
bool DPvzFile::truncate(int64_t idx, int32_t root)
#else
bool DPvzFile::truncate(int64_t idx)
#endif
/****************************************************************************/
/*                                                                          */
/* truncate the file from a specific time step onward. the indicated time   */
/* step will be removed. truncate(0) empties the file of all time steps,    */
/* the global and metadata remain.                                          */
/*                                                                          */
/* THIS IS A COLLECTIVE COMMUNICATION ROUTINE.                              */
/* IT REQUIRES ALL RANKS TO PARTICIPATE.                                    */
/*                                                                          */
/* return true if the call fails, false otherwise.                          */
/*                                                                          */
/****************************************************************************/
{
  DPvzTocIndex toc_idx;
  DPvzTocIndex last_idx;

  reset_err();

  if (!failed() && mode == DPvzReadOnly) {
    // file mode appears to be read-only
    fprintf(stderr, "%s: %4d: truncate: attempt to truncate with mode == Read Only, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    set_err(DPvzSoft, check1);
  }

#if defined(DPVZ_MPI)
  MPI_Bcast(&idx, 1, MPI_INT64_T, root, comm);
#endif

  if (!failed() && (idx < 0 || metadata->active_entries <= idx)) {
    // attempting to truncate a record that is not in the file
    fprintf(stderr, "%s: %4d: truncate: index (%ld) is out of range, file='%s'\n", name_only(__FILE__), __LINE__, idx, file_name.c_str());
    set_err(DPvzSoft, check1);
  }

  if (!failed()) {
    // find the entry where the file will be truncated
    toc_idx = toc->get_idx(idx);
    if (toc_idx.table < 0 || 2 < toc_idx.table) {
      // we should never get an invalid table number like this, no matter what
      fprintf(stderr, "%s: %4d: truncate: internal error, toc table (%d) is out of range, file='%s'\n", name_only(__FILE__), __LINE__, toc_idx.table, file_name.c_str());
      set_err(DPvzHard, check1);
    }
  }

  if (!failed()) {
    // find the last entry in the table of contents
    last_idx = toc->get_idx(metadata->active_entries);
    if (last_idx.table < 0 || 2 < last_idx.table) {
      // we should never get an invalid table number like this, no matter what
      fprintf(stderr, "%s: %4d: truncate: internal error, last table (%d) is out of range, file='%s'\n", name_only(__FILE__), __LINE__, last_idx.table, file_name.c_str());
      set_err(DPvzHard, check1);
    }
  }

  if (!failed()) {
    if (last_idx.table < toc_idx.table) {
      // something has hosed up either the last entry or the table of contents
      fprintf(stderr, "%s: %4d: truncate: internal error, last table (%d) is less than idx table (%d), file='%s'\n", name_only(__FILE__), __LINE__, last_idx.table, toc_idx.table, file_name.c_str());
      set_err(DPvzHard, check1);
    }
  }

check1:
#if defined(DPVZ_MPI)
  MPI_Allreduce(MPI_IN_PLACE, &_err_stat, 1, MPI_INT, MPI_LOR, comm);
  if (failed()) {
    MPI_Allreduce(MPI_IN_PLACE, &_err_line, 1, MPI_INT, MPI_MAX, comm);
    fprintf(stderr, "%s: %4d: truncate: error truncating file, err=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
    return true;
  }
#else
  if (failed()) {
    return true;
  }
#endif

  // toc_idx and last_idx are valid from this point forward

  // find the offset where the file and table will be truncated
  int64_t truncate_offset = metadata->total_file_size;
  if (toc_idx.table == 0) {
    truncate_offset = toc->L0[toc_idx.l0_idx].offset;

    // it isn't strictly necessary to reset the entries, but it may help catch some errors to do so
    if (last_idx.table == 0) {
      // toc_idx.table and last_idx.table both are zero, reset L0
      for (int i=toc_idx.l0_idx; i <= last_idx.l0_idx; i++) {
	toc->L0[i] = DPvzTocEntry::unused;
      }
    } else if (last_idx.table == 1) {
      // toc_idx.table is zero and last_idx.table is one, reset both L0 and L1
      for (int i=toc_idx.l0_idx; i < DPvzToc::table_len; i++) {
	toc->L0[i] = DPvzTocEntry::unused;
      }
      for (int i=0; i < last_idx.l1_idx; i++) {
	toc->L1[i] = DPvzTocEntry::unused;
      }
    } else if (last_idx.table == 2) {
      // toc_idx.table is zero and last_idx.table is two, reset L0, L1, and L2
      for (int i=toc_idx.l0_idx; i < DPvzToc::table_len; i++) {
	toc->L0[i] = DPvzTocEntry::unused;
      }
      for (int i=0; i < DPvzToc::table_len; i++) {
	toc->L1[i] = DPvzTocEntry::unused;
      }
      for (int i=0; i < last_idx.l2_idx; i++) {
	toc->L2[i] = DPvzTocEntry::unused;
      }
    }
  } else if (toc_idx.table == 1) {
    // get the file truncation offset
    if (toc_idx.l0_idx == 0) {
      // we're deleting a whole L1 entry, the truncation offset is in L1
      truncate_offset = toc->L1[toc_idx.l1_idx].offset;
      toc->L1[toc_idx.l1_idx] = DPvzTocEntry::unused;
    } else {
      // we're deleting a partial L1 entry
      // we have to read the offset from an L0 entry and it's OK to read the file in parallel
      int64_t L0_offset = toc->L1[toc_idx.l1_idx].offset;
      DPvzTocEntry mem_L0[DPvzToc::table_len];

      // seek to the L0 on disk
      if (lseek(fd, L0_offset, SEEK_SET) < 0) {
	// for some reason we can't do a seek on the file, so bail...
	fprintf(stderr, "%s: %4d: truncate: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("lseek");
	set_err(DPvzHard, check2);
      }

      // read the L0
      if (! failed() && ::read(fd, mem_L0, sizeof mem_L0) < (ssize_t) sizeof mem_L0) {
	// for some reason we can't read the file, so bail...
	fprintf(stderr, "%s: %4d: truncate: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("read");
	set_err(DPvzHard, check2);
      }

      truncate_offset = mem_L0[toc_idx.l0_idx].offset;
    }

    // it isn't strictly necessary to reset the entries, but it may help catch some errors to do so
    if (last_idx.table == 1) {
      for (int i=toc_idx.l1_idx+1; i <= last_idx.l1_idx; i++) {
	toc->L1[i] = DPvzTocEntry::unused;
      }
    } else if (last_idx.table == 2) {
      for (int i=toc_idx.l1_idx+1; i < DPvzToc::table_len; i++) {
	toc->L1[i] = DPvzTocEntry::unused;
      }
      for (int i=0; i < last_idx.l1_idx; i++) {
	toc->L2[i] = DPvzTocEntry::unused;
      }
    }
  } else if (toc_idx.table == 2) {
    // get the file truncation offset
    if (toc_idx.l1_idx == 0 && toc_idx.l0_idx == 0) {
      // we're deleting a whole L2 entry, the truncation offset is in L2
      truncate_offset = toc->L2[toc_idx.l2_idx].offset;
      toc->L2[toc_idx.l2_idx] = DPvzTocEntry::unused;
    } else if (toc_idx.l0_idx == 0) {
      // we have to read the L1, but not the L0
      int64_t L1_offset = toc->L2[toc_idx.l2_idx].offset;
      DPvzTocEntry mem_L1[DPvzToc::table_len];

      // seek to the L1 on disk
      if (lseek(fd, L1_offset, SEEK_SET) < 0) {
	// for some reason we can't do a seek on the file, so bail...
	fprintf(stderr, "%s: %4d: truncate: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("lseek");
	set_err(DPvzHard, check2);
      }

      // read the L1
      if (! failed() && ::read(fd, mem_L1, sizeof mem_L1) < (ssize_t) sizeof mem_L1) {
	// for some reason we can't read the file, so bail...
	fprintf(stderr, "%s: %4d: truncate: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("read");
	set_err(DPvzHard, check2);
      }

      truncate_offset = mem_L1[toc_idx.l1_idx].offset;
    } else {
      // we have to read the L1 and the L0
      int64_t L1_offset = toc->L2[toc_idx.l2_idx].offset;
      DPvzTocEntry mem_L1[DPvzToc::table_len];

      // seek to the L1 on disk
      if (lseek(fd, L1_offset, SEEK_SET) < 0) {
	// for some reason we can't do a seek on the file, so bail...
	fprintf(stderr, "%s: %4d: truncate: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("lseek");
	set_err(DPvzHard, check2);
      }

      // read the L1
      if (! failed() && ::read(fd, mem_L1, sizeof mem_L1) < (ssize_t) sizeof mem_L1) {
	// for some reason we can't read the file, so bail...
	fprintf(stderr, "%s: %4d: truncate: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("read");
	set_err(DPvzHard, check2);
      }

      DPvzTocEntry mem_L0[DPvzToc::table_len];
      int64_t L0_offset = mem_L1[toc_idx.l1_idx].offset;

      // seek to the L0 on disk
      if (lseek(fd, L0_offset, SEEK_SET) < 0) {
	// for some reason we can't do a seek on the file, so bail...
	fprintf(stderr, "%s: %4d: truncate: unable to seek within file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("lseek");
	set_err(DPvzHard, check2);
      }

      // read the L0
      if (! failed() && ::read(fd, mem_L0, sizeof mem_L0) < (ssize_t) sizeof mem_L0) {
	// for some reason we can't read the file, so bail...
	fprintf(stderr, "%s: %4d: truncate: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
	perror("read");
	set_err(DPvzHard, check2);
      }

      truncate_offset = mem_L0[toc_idx.l0_idx].offset;
    }

    // it isn't strictly necessary to reset the entries, but it may help catch some errors to do so
    for (int i=toc_idx.l2_idx+1; i <= last_idx.l2_idx; i++) {
      toc->L2[i] = DPvzTocEntry::unused;
    }
  }

check2:
#if defined(DPVZ_MPI)
  MPI_Allreduce(MPI_IN_PLACE, &_err_stat, 1, MPI_INT, MPI_LOR, comm);
  if (failed()) {
    MPI_Allreduce(MPI_IN_PLACE, &_err_line, 1, MPI_INT, MPI_MAX, comm);
    fprintf(stderr, "%s: %4d: truncate: error truncating file, err=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
    return true;
  }
#else
  if (failed()) {
    return true;
  }
#endif

  // update the metadata
  metadata->total_file_size = truncate_offset;
  metadata->active_entries  = idx;

  // have only one rank actually truncate the file
  if (rank == 0) {
    if (::ftruncate(fd, truncate_offset) < 0) {
      // for some reason we can't truncate the file, so bail...
      fprintf(stderr, "%s: %4d: write: unable to truncate, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      perror("ftruncate");
      set_err(DPvzHard, check3);
    }

    if (!failed()) {
      // write the metadata back to the file
      metadata->write(fd, file_name);
    }
  }

check3:
#if defined(DPVZ_MPI)
  MPI_Allreduce(MPI_IN_PLACE, &_err_stat, 1, MPI_INT, MPI_LOR, comm);
  if (failed()) {
    MPI_Allreduce(MPI_IN_PLACE, &_err_line, 1, MPI_INT, MPI_MAX, comm);
    fprintf(stderr, "%s: %4d: truncate: error truncating file, err=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
    return true;
  }
#else
  if (failed()) {
    return true;
  }
#endif

  return failed();
}

/****************************************************************************/
#if defined(DPVZ_MPI)
bool DPvzFile::truncate(int64_t cycle, double time, int32_t root)
#else
bool DPvzFile::truncate(int64_t cycle, double time)
#endif
/****************************************************************************/
/*                                                                          */
/* truncate the file from a specific time step onward. the indicated time   */
/* step and all subsequent time steps will be removed. the parameters are   */
/* the cycle number and simulation time. all time steps whose cycle number  */
/* or simulation time are equal to or later than EITHER of the parameter    */
/* cycle number or simulation time will be removed. e.g.,                   */
/* (1) truncate(0,0) empties the file of all time steps                     */
/* (2) truncate(500, DBL_MAX) removes all time steps including and after    */
/*     cycle 500                                                            */
/* (3) truncate(INT_MAX, 0.25) removes all time steps including and after   */
/*     simulation time 0.25                                                 */
/* (4) truncate(500, 0.25) removes all time steps starting from cycle 500   */
/*     or time 0.25, whichever is less                                      */
/* note: the parallel version is a collective operation.                    */
/* note: this is a convenience function for truncate(int64_t idx)           */
/*                                                                          */
/* THIS IS A COLLECTIVE COMMUNICATION ROUTINE.                              */
/* IT REQUIRES ALL RANKS TO PARTICIPATE.                                    */
/*                                                                          */
/* return true if the call fails, false otherwise.                          */
/* note: requesting truncation for a time step that does not exist is not   */
/*       a failure, i.e., it returns false.                                 */
/*                                                                          */
/****************************************************************************/
{
  reset_err();

  if (cycle != (int64_t) ((int32_t) cycle)) {
    fprintf(stderr, "%s: %4d: write: attempt to write a cycle larger than 2,147,483,647 (0x7FFFFFFF), file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    return true;
  }

  // get the table of contents
  int64_t steps = get_steps();
  if (0 < steps) {
    DPvzTocEntry* map = new DPvzTocEntry[steps];
    if (map == NULL) {
      fprintf(stderr, "%s: %4d: truncate: failed to allocate memory for a map, err=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
      return true;
    }

    if (get_map(map)) {
      fprintf(stderr, "%s: %4d: truncate: failed to get a map, err=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
      delete [] map;		// free up the space we allocated but no longer need
      return true;
    }

    // linear search to find the first time step (ts) where cycle <= ts.cycle OR time <= ts.time
    int64_t idx = -1;
    for (int64_t i=0; i < steps; i++) {
      if (cycle <= map[i].cycle || time <= map[i].time) {
	idx = i;
	break;
      }
    }

    // we're done with map, so free up the space
    delete [] map;

#if defined(DPVZ_MPI)
    // make sure everyone agrees on the index
    MPI_Bcast(&idx, 1, MPI_INT64_T, root, comm);
    if (0 <= idx && idx < steps) {
      // we have a valid index, so truncate from that index forward
      if (DPvzFile::truncate(idx, root)) {
	// truncate failed, unable to truncate a valid time step
	fprintf(stderr, "%s: %4d: truncate: error truncating file, err=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
	return true;
      }
    } else {
      // no index matches the criteria, we succeed vacuously
      return false;
    }
#else
    if (0 <= idx && idx < steps) {
      // we have a valid index, so truncate from that index forward
      if (DPvzFile::truncate(idx)) {
	// truncate failed, unable to truncate a valid time step
	fprintf(stderr, "%s: %4d: truncate: error truncating file, err=%d, file='%s'\n", name_only(__FILE__), __LINE__, _err_line, file_name.c_str());
	return true;
      }
    } else {
      // no index matches the criteria, we succeed vacuously
      return false;
    }
#endif
  }

  // nothing in the file to truncate
  return false;
}


/****************************************************************************/
/*                                                                          */
/*                             Private Methods                              */
/*                                                                          */
/****************************************************************************/

/****************************************************************************/
bool DPvzFile::read()
/****************************************************************************/
/*                                                                          */
/* read the metadata, table of contents, and global data sections from a    */
/* file. this is expected to work only on newly created DPvzFile structures */
/* but tests are in place whether they already exist, and if so, to delete  */
/* them, just in case.                                                      */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/* return true if the operation fails for some reason, false otherwise.     */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: read: entering\n", name_only(__FILE__), __LINE__);
#endif

  if (fd < 0) {
    fprintf(stderr, "%s: %4d: read: invalid file descriptor for file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    return true;
  }

  bool err = false;

  // if they exist, delete the following first to avoid a memory leak
  if (metadata != NULL) delete metadata;
  if (toc      != NULL) delete toc;
  if (global   != NULL) delete global;

  // read the metadata
  metadata = new DPvzMetadata(fd, file_name, err);
  if (err) {
    fprintf(stderr, "%s: %4d: read: unable to read metadata, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    if (metadata != NULL) delete metadata;
    metadata = NULL;
    return true;
  }

  // read the global data
  global = new DPvzGlobal(fd, metadata, file_name, err);
  if (err) {
    fprintf(stderr, "%s: %4d: read: unable to read global data, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    if (global != NULL) delete global;
    global = NULL;
    return true;
  }

  // read the table of contents
  toc = new DPvzToc(fd, metadata, file_name, err);
  if (err) {
    fprintf(stderr, "%s: %4d: read: unable to read table of contents, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    if (toc != NULL) delete toc;
    toc = NULL;
    return true;
  }

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: read: leaving\n", name_only(__FILE__), __LINE__);
#endif

  return false;
}

/****************************************************************************/
bool DPvzFile::validate(uint64_t g_sz, std::string majik, bool repair, int root)
/****************************************************************************/
/*                                                                          */
/* validate the following:                                                  */
/*    1. majik string                                                       */
/*    2. file length                                                        */
/*    3. metadata checksum                                                  */
/*    4. global data checksum                                               */
/*    5. table of contents checksum                                         */
/*    6. global data length                                                 */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY, BUT REPAIR IS ONLY   */
/* EXECUTED WHEN AN ERROR IS FOUND AND RANK == ROOT OR NUMBER_OF_RANKS == 1.*/
/* (RANK == 0 and NUMBER_OF_RANKS == 1 FOR NON-MPI EXECUTION.)              */
/*                                                                          */
/* repair can address the following issues:                                 */
/*    1. file length field in metadata                                      */
/*    2. metadata checksum                                                  */
/*    3. global data checksum                                               */
/*    4. table of contents checksum                                         */
/*    5. partially written time step entries                                */
/*                                                                          */
/* see issue #516 for details.                                              */
/*                                                                          */
/****************************************************************************/
{
  bool rv = false;

  // validate secondary file string
  if (majik.c_str() != NULL && strncmp((const char*) metadata->majik, (const char*) majik.c_str(), sizeof metadata->majik) != 0) {
    fprintf(stderr, "%s: %4d: validate: recorded majik string (%s) doesn't match actual majik (%s), file '%s'\n", name_only(__FILE__), __LINE__, metadata->majik, majik.c_str(), file_name.c_str());
    rv = true;
  }

  // validate file length
  off_t eof = lseek(fd, 0L, SEEK_END);
  if (eof != (off_t) metadata->total_file_size) {
    fprintf(stderr, "%s: %4d: validate: recorded file length (%ld) doesn't match actual length (%ld), file '%s'\n", name_only(__FILE__), __LINE__, metadata->total_file_size, eof, file_name.c_str());
    rv = true;
  }

  // validate metadata checksum
  uint64_t tmp_metadata_checksum = metadata->metadata_checksum;
  metadata->metadata_checksum = 0L;
  uint64_t new_metadata_checksum = crc64(metadata, metadata->metadata_only);
  metadata->metadata_checksum = tmp_metadata_checksum;
  if (new_metadata_checksum != metadata->metadata_checksum) {
    fprintf(stderr, "%s: %4d: validate: recorded metadata checksum (%lx) doesn't match actual checksum (%lx), file '%s'\n", name_only(__FILE__), __LINE__, metadata->metadata_checksum, new_metadata_checksum, file_name.c_str());
    rv = true;
  }

  // validate global data checksum
  uint64_t new_global_checksum = crc64(global->data, metadata->global_only);
  if (new_global_checksum != metadata->global_checksum) {
    fprintf(stderr, "%s: %4d: validate: recorded global checksum (%lx) doesn't match actual checksum (%lx), file '%s'\n", name_only(__FILE__), __LINE__, metadata->global_checksum, new_global_checksum, file_name.c_str());
    rv = true;
  }

  // validate table of contents checksum
  uint64_t new_toc_checksum = crc64(toc, metadata->toc_only);
  if (new_toc_checksum != metadata->toc_checksum) {
    fprintf(stderr, "%s: %4d: validate: recorded table of contents checksum (%lx) doesn't match actual checksum (%lx), file '%s'\n", name_only(__FILE__), __LINE__, metadata->toc_checksum, new_toc_checksum, file_name.c_str());
    rv = true;
  }

  // validate global data length
  if (g_sz != metadata->global_only) {
    fprintf(stderr, "%s: %4d: validate: recorded global size (%ld) doesn't match expected size (%ld), file '%s'\n", name_only(__FILE__), __LINE__, metadata->global_only, g_sz, file_name.c_str());
    rv = true;
  }

  // is it time to quit and go home?
  if (! rv || ! repair) {
    // return if (a) we did not find an error, or (b) we are not told to attempt a repair
    return rv;
  }

  // DMP: TODO implement DPvz file repair, see issue #516
  // we are about to attempt to repair a broken/borked file

  return rv;
}

/****************************************************************************/
bool DPvzFile::write()
/****************************************************************************/
/*                                                                          */
/* write the global data, table of contents, and metadata to a file.        */
/*                                                                          */
/* THIS IS A LOCAL OPERATION. IT IS EXECUTED SERIALLY.                      */
/*                                                                          */
/* return true if the operation fails for some reason, false otherwise.     */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: write: entering, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
#endif

    // write the global data
    if (global->write(fd, metadata, file_name)) {
      fprintf(stderr, "%s: %4d: read: unable to write global data, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      return true;
    }

    // write the table of contents
    if (toc->write(fd, metadata, file_name)) {
      fprintf(stderr, "%s: %4d: read: unable to write table of contents, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      return true;
    }

    // write the metadata
    if (metadata->write(fd, file_name)) {
      fprintf(stderr, "%s: %4d: read: unable to write metadata, file '%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
      return true;
    }

#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: write: leaving, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
#endif

  return false;
}
