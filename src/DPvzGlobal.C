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

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <poll.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/wait.h>

#include "DPvzUtil.h"

#define min(x,y)	(((x)<=(y)) ? (x) : (y))


/****************************************************************************/
/*                                                                          */
/*                                DPvzGlobal                                */
/*                                                                          */
/****************************************************************************/

/****************************************************************************/
DPvzGlobal::DPvzGlobal(DPvzMetadata* meta)
/****************************************************************************/
/*                                                                          */
/* allocate a buffer for the global data. the buffer size is provided by    */
/* the metadata structure. the unpadded size is in global_only, the padded  */
/* size, which is what we allocate, is given in global_size.                */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: DPvzGlobal: entering\n", name_only(__FILE__), __LINE__);
#endif
  data = new uint8_t[meta->global_size];
  memset(data, 0xff, meta->global_size);
  memset(data, '\0', meta->global_size);
//fprint(stdout, meta);
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: DPvzGlobal: leaving\n", name_only(__FILE__), __LINE__);
#endif
}

/****************************************************************************/
DPvzGlobal::DPvzGlobal(int fd, DPvzMetadata* meta, std::string file_name, bool& err)
/****************************************************************************/
/*                                                                          */
/* allocate a buffer for the global data. the buffer size is provided by    */
/* the metadata structure. the unpadded size is in global_only, the padded  */
/* size, which is what we allocate, is given in global_size.                */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: DPvzGlobal: entering, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
#endif
  data = new uint8_t[meta->global_size];
  memset(data, 0xff, meta->global_size);
  memset(data, '\0', meta->global_size);
  if (read(fd, meta, file_name)) {
    fprintf(stderr, "%s: %4d: DPvzGlobal: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    err = true;
    return;
  }
//fprint(stdout, meta);
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: DPvzGlobal: leaving\n", name_only(__FILE__), __LINE__);
#endif
}

/****************************************************************************/
DPvzGlobal::~DPvzGlobal()
/****************************************************************************/
/*                                                                          */
/* deallocate the global data buffer.                                       */
/*                                                                          */
/****************************************************************************/
{
  delete [] data;
}

/****************************************************************************/
bool DPvzGlobal::read (int fd, DPvzMetadata* meta, std::string file_name)
/****************************************************************************/
/*                                                                          */
/* read the entire global section into a pre-allocated buffer. the location */
/* on disk is stored in the metadata structure.                             */
/*                                                                          */
/* return true if an error is detected, false otherwise.                    */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: entering fd=%d meta=%p, file='%s'\n", name_only(__FILE__), __LINE__, fd, meta, file_name.c_str());
#endif

  // do we have a valid file descriptor and metadata section? if not, bail...
  if (fd < 0 || meta == NULL) {
    return true;
  }

  // check whether there is anything here to read
  if (meta->global_only < 1) {
    // nothing to read, but it's not an error
    return false;
  }

  // go to the global section in the file
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: lseek fd=%d meta->global_offset=%ld, file='%s'\n", name_only(__FILE__), __LINE__, fd, meta->global_offset, file_name.c_str());
#endif
  if (lseek(fd, meta->global_offset, SEEK_SET) < 0) {
    fprintf(stderr, "%s: %4d: read: unable to seek, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    return true;
  }

  // read the entire global section
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: read fd=%d data=%p meta->global_only=%ld, file='%s'\n", name_only(__FILE__), __LINE__, fd, data, meta->global_only, file_name.c_str());
#endif
  if (::read(fd, data, meta->global_only) < (ssize_t) meta->global_only) {
    fprintf(stderr, "%s: %4d: read: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("read");
    return true;
  }

  // check the global section checksum
  uint64_t meta_global_checksum = meta->global_checksum;
  uint64_t data_global_checksum = crc64((const uint8_t*) data, meta->global_only);
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: crc64 meta=%lx data=%lx, file='%s'\n", name_only(__FILE__), __LINE__, meta_global_checksum, data_global_checksum, file_name.c_str());
#endif
  if (meta_global_checksum != 0 && meta_global_checksum != data_global_checksum) {
    fprintf(stderr, "%s: %4d: read: global checksums don't match meta=%llx != file=%llx, file='%s'\n", name_only(__FILE__), __LINE__, meta_global_checksum, data_global_checksum, file_name.c_str());
    return true;
  }

#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: leaving, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
#endif

  return false;
}

/****************************************************************************/
bool DPvzGlobal::set_data(DPvzMetadata* meta, void* buf, ssize_t size, off_t offset)
/****************************************************************************/
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: set_data: entering\n", name_only(__FILE__), __LINE__);
#endif
  if (buf == NULL || meta->global_only < (uint64_t) (offset+size)) {
    return true;
  }

  // check whether there is anything here to set
  if (meta->global_only < 1) {
    // nothing to set, but it's not an error
    return false;
  }

#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: set_data: min size=%ld only=%ld\n", name_only(__FILE__), __LINE__, size, meta->global_only);
#endif
  ssize_t sz = min((uint64_t) size, (uint64_t) (meta->global_only - offset));
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: set_data: data=%p\n", name_only(__FILE__), __LINE__, data); fflush(stdout);
fprintf(stdout, "%-16s: %4d: set_data: data+offset=%p\n", name_only(__FILE__), __LINE__, data+offset); fflush(stdout);
fprintf(stdout, "%-16s: %4d: set_data: buf=%p\n", name_only(__FILE__), __LINE__, buf); fflush(stdout);
fprintf(stdout, "%-16s: %4d: set_data: sz=%ld\n", name_only(__FILE__), __LINE__, sz); fflush(stdout);
#endif

#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: set_data: memcpy data+offset=%ld buf=%p sz=%ld\n", name_only(__FILE__), __LINE__, data+offset, buf, sz);
#endif
  memcpy(data+offset, buf, sz);

  // generate the global checksum
  meta->global_checksum = crc64((const uint8_t*) data, meta->global_only);
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: set_data: checksum=%lu\n", name_only(__FILE__), __LINE__, meta->global_checksum);
fprintf(stdout, "%-16s: %4d: set_data: leaving\n", name_only(__FILE__), __LINE__);
#endif

  return false;
}

/****************************************************************************/
bool DPvzGlobal::write(int fd, DPvzMetadata* meta, std::string file_name)
/****************************************************************************/
/*                                                                          */
/* write the global data section, which is stored in a pre-allocated buffer,*/
/* to a file. the location on disk is stored in the metadata structure.     */
/*                                                                          */
/* return true if an error is detected, false otherwise.                    */
/*                                                                          */
/****************************************************************************/
{
  // do we have a valid file descriptor and metadata section? if not, bail...
  if (fd < 0 || (fcntl(fd, F_GETFL) & O_RDWR) == 0 || meta == NULL) {
    fprintf(stderr, "%s: %4d: write: bad file descriptor fd=%d or metadata meta=%p, file='%s'\n", name_only(__FILE__), __LINE__, fd, meta, file_name.c_str());
    return true;
  }

  // go to the start of the global section
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: seek fd=%d to global offset=%ld, file='%s'\n", name_only(__FILE__), __LINE__, fd, meta->global_offset, file_name.c_str());
#endif
  if (lseek(fd, meta->global_offset, SEEK_SET) < 0) {
    fprintf(stderr, "%s: %4d: write: unable to seek, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    return true;
  }

  // write the global section
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: write size=%ld to fd=%d offset=%ld, file='%s'\n", name_only(__FILE__), __LINE__, meta->global_size, fd, meta->global_offset, file_name.c_str());
#endif
  if (::write(fd, data, meta->global_size) < (ssize_t) meta->global_size) {
    fprintf(stderr, "%s: %4d: write: unable to write, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("write");
    return true;
  }

  // generate the global checksum
  meta->global_checksum = crc64((const uint8_t*) data, meta->global_only);
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: checksum=%lx, file='%s'\n", name_only(__FILE__), __LINE__, meta->global_checksum, file_name.c_str());
#endif

  return false;
}

/****************************************************************************/
void DPvzGlobal::fprint(FILE* fp, DPvzMetadata* meta)
/****************************************************************************/
/****************************************************************************/
{
  ssize_t size = meta->global_only;
#if defined(DPvzTrace)
fprintf(fp, "%-16s: %4d: fprint: DPvzGlobal (%ld)\n", name_only(__FILE__), __LINE__, size);
#endif
  fprintf(fp, "DPvzGlobal (%ld)\n", size);
  char* buf = (char*) data;
  fdump(fp, buf, size);
}
