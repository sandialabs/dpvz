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

#include "DPvzMetadata.h"

#include <stdio.h>
// #include <string.h>
// #include <strings.h>
#include <unistd.h>
#include <poll.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/wait.h>

#include "DPvzUtil.h"

// Distributed (D) Parallel (P) visualization (v) compressed (z) File -- DPvzFile (.dpvz)

#define min(x,y)	(((x)<=(y)) ? (x) : (y))


/****************************************************************************/
/*                                                                          */
/*                               DPvzMetadata                               */
/*                                                                          */
/****************************************************************************/

/****************************************************************************/
DPvzMetadata::DPvzMetadata(uint64_t global_size_in_bytes, const char* majik)
/*                                                                          */
/* allocate a new metadata structure. the size of the global data structure */
/* is given in global_size_in_bytes, the secondary magic string is given in */
/* majik.                                                                   */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzMetadata: entering\n", name_only(__FILE__), __LINE__);
#endif
  set_magic("DPvzFile");
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzMetadata: set majik='%s'\n", name_only(__FILE__), __LINE__, (majik==NULL)?"null":majik);
#endif
  set_majik(majik);
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzMetadata: init global magic='%s' majik='%s'\n", name_only(__FILE__), __LINE__, (magic==NULL)?"null":(const char*)magic, (majik==NULL)?"null":(const char*)majik);
#endif
  init(global_size_in_bytes);
// fprint(stdout);
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: DPvzMetadata: leaving\n", name_only(__FILE__), __LINE__);
#endif
}

/****************************************************************************/
DPvzMetadata::DPvzMetadata(int fd, std::string file_name, bool& err)
/*                                                                          */
/* allocate a new metadata structure. the size of the global data structure */
/* is given in global_size_in_bytes, the secondary magic string is given in */
/* majik.                                                                   */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: DPvzMetadata: entering, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
#endif
  if (read(fd, file_name)) {
    fprintf(stderr, "%s: %4d: DPvzMetadata: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    err = true;
    return;
  }
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: DPvzMetadata: leaving, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
#endif
}

/****************************************************************************/
void DPvzMetadata::init(uint64_t global_size_in_bytes)
/*                                                                          */
/* init is a private function that is used to initialize a new metadata     */
/* structure. metadata describes the characteristics of the visualization   */
/* file, including the software version, primary and secondary magic        */
/* strings, the size and location on disk of the global data and table of   */
/* contents sections, etc.                                                  */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: init: entering\n", name_only(__FILE__), __LINE__);
#endif

  byte_order        = DPVZ_BYTE_ORDER;
  major             = DPVZ_MAJOR;
  minor             = DPVZ_MINOR;
  build             = DPVZ_BUILD;
  revision          = DPVZ_REVISION;
  page_size         = DPVZ_PAGE_SIZE;

  // TODO: do we want to page align the metadata, toc, and global data,
  // or do we want to pack them together?
  // metadata_offset   = 0L;
  metadata_only     = sizeof(DPvzMetadata);
  metadata_size     = ((metadata_only + page_size - 1) / page_size) * page_size;	// page aligned size
  metadata_size     = metadata_only;							// packed size
  metadata_checksum = 0L;

  // since metadata is less than a full page and ToC equals 3 pages, pack the global section after the metadata
  // and pad the size so that ToC starts on a page boundary
  global_offset     = metadata_offset + metadata_size;
  global_only       = (global_size_in_bytes < 0) ? 0 : global_size_in_bytes;		// unpadded size is at least zero
  global_size       = ((global_offset + global_only + page_size - 1) / page_size) * page_size - global_offset;	// size rounded to end on a page
  global_checksum   = 0L;

  toc_offset        = global_offset + global_size;
  toc_only          = sizeof(DPvzToc);
  toc_size          = ((toc_only + page_size - 1) / page_size) * page_size;		// page aligned size
  toc_size          = toc_only;								// packed size
  toc_checksum      = 0L;
  active_entries    = 0L;
  last_entry        = DPvzToc::unused_entry;

  total_file_size   = toc_offset + toc_size;
  data_offset       = total_file_size;

  compression_algorithm = ZLIB_7;

// fprint(stdout);
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: init: leaving\n", name_only(__FILE__), __LINE__);
#endif
}

/****************************************************************************/
DPvzMetadata::~DPvzMetadata()
/*                                                                          */
/* close out the metadata section and deallocate any storage associated     */
/* with it.                                                                 */
/*                                                                          */
/****************************************************************************/
{
  // nothing to deallocate, all data should already be written to the file
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: ~DPvzMetadata: entering and leaving\n", name_only(__FILE__), __LINE__);
#endif
}

/****************************************************************************/
bool DPvzMetadata::read(int fd, std::string file_name)
/*                                                                          */
/* read the metadata section from a file descriptor.                        */
/*                                                                          */
/* return true if there were errors, false otherwise                        */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: entering\n", name_only(__FILE__), __LINE__);
#endif

  // do we have a valid file descriptor? if not, bail...
  if (fd < 0) {
    return true;
  }

  // go to the start of the file, i.e., the metadata section
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: lseek fd=%d offset=%ld\n", name_only(__FILE__), __LINE__, fd, metadata_offset);
#endif
  if (lseek(fd, metadata_offset, SEEK_SET) < 0) {
    fprintf(stderr, "%s: %4d: read: unable to seek, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    return true;
  }

  // read the metadata section
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: read fd=%d this=%p size=%ld\n", name_only(__FILE__), __LINE__, fd, this, sizeof *this);
#endif
  if (::read(fd, this, sizeof *this) < (ssize_t) sizeof *this) {
    fprintf(stderr, "%s: %4d: read: unable to read, fd=%d, file='%s'\n", name_only(__FILE__), __LINE__, fd, file_name.c_str());
    perror("read");
    return true;
  }

  // check the metadata checksum
  uint64_t file_checksum = metadata_checksum;
  metadata_checksum = 0L;
  metadata_checksum = crc64((const uint8_t*) this, sizeof *this);
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: crc64 file=%lx meta=%lx, file='%s'\n", name_only(__FILE__), __LINE__, file_checksum, metadata_checksum, file_name.c_str());
#endif
  if (file_checksum != 0 && file_checksum != metadata_checksum) {
    fprintf(stderr, "%s: %4d: read: metadata checksums don't match, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    return true;
  }

// fprint(stdout);
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: leaving\n", name_only(__FILE__), __LINE__);
#endif

  return false;
}

/****************************************************************************/
bool DPvzMetadata::write(int fd, std::string file_name)
/*                                                                          */
/* write the metadata section to a file descriptor.                         */
/*                                                                          */
/* return true if there were errors, false otherwise                        */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: entering, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
#endif
  // do we have a valid file descriptor? if not, bail...
  if (fd < 0 || (fcntl(fd, F_GETFL) & O_RDWR) == 0) {
    return true;
  }

  // generate the metadata checksum
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: crc64 this=%p size=%ld, file='%s'\n", name_only(__FILE__), __LINE__, this, sizeof *this, file_name.c_str());
#endif
  metadata_checksum = 0L;
  metadata_checksum = crc64((const uint8_t*) this, sizeof *this);
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: checksum=%lx, file='%s'\n", name_only(__FILE__), __LINE__, metadata_checksum, file_name.c_str());
#endif

// fprint(stdout);
//  fdump(stdout);

  // go to the metadata section (start of the file)
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: seek fd=%d offset=%ld, file='%s'\n", name_only(__FILE__), __LINE__, fd, metadata_offset, file_name.c_str());
#endif
  if (lseek(fd, metadata_offset, SEEK_SET) < 0) {
    fprintf(stderr, "%s: %4d: write: unable to seek, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    return true;
  }

  // write the metadata section
  ssize_t size = sizeof *this;
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: write fd=%d, class size=%ld, file='%s'\n", name_only(__FILE__), __LINE__, fd, size, file_name.c_str());
#endif
  if (::write(fd, (const char*) this, (size_t) size) < size) {
    fprintf(stderr, "%s: %4d: write: unable to write, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("write");
    return true;
  }

#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: leaving, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
#endif

  return false;
}

static void prt_mem(FILE* fp, char* m, int size)
{
  fprintf(fp, "\"");
  for (int i=0; i < size; i++) {
    if (' ' <= m[i] && m[i] < 0x7f) {
      fprintf(fp, "%c", m[i]);
    } else {
      fprintf(fp, ".");
    }
  }
  fprintf(fp, "\"");

  fprintf(fp, " ");

  for (int i=0; i < size; i++) {
    fprintf(fp, " %02x", m[i]);
  }
  fprintf(fp, "\n");
}

/****************************************************************************/
void DPvzMetadata::fprint(FILE* fp)
/****************************************************************************/
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: fprint: entering\n", name_only(__FILE__), __LINE__);
#endif
  fprintf(fp, "DPvzMetadata\n");
  fprintf(fp, "  magic:             "); prt_mem(fp, (char*) magic, sizeof(majik));
  fprintf(fp, "  majik:             "); prt_mem(fp, (char*) majik, sizeof(majik));
  fprintf(fp, "  byte order:        %ld\n", byte_order);
  fprintf(fp, "  version:           %d.%d.%d.%d\n", major, minor, build, revision);
  fprintf(fp, "  page size:         %8ld\n", page_size);
  fprintf(fp, "  total file size:   %8ld\n", total_file_size);
  fprintf(fp, "  active entries:    %8ld (%lx)\n", active_entries,    active_entries);
  fprintf(fp, "  metadata offset:   %8ld (%lx)\n", metadata_offset,   metadata_offset);
  fprintf(fp, "  metadata only:     %8ld (%lx)\n", metadata_only,     metadata_only);
  fprintf(fp, "  metadata size:     %8ld (%lx)\n", metadata_size,     metadata_size);
  fprintf(fp, "  metadata checksum: %016lx\n",    metadata_checksum);
  fprintf(fp, "  global offset:     %8ld (%lx)\n", global_offset,     global_offset);
  fprintf(fp, "  global only:       %8ld (%lx)\n", global_only,       global_only);
  fprintf(fp, "  global size:       %8ld (%lx)\n", global_size,       global_size);
  fprintf(fp, "  global checksum:   %016lx\n",    global_checksum);
  fprintf(fp, "  toc offset:        %8ld (%lx)\n", toc_offset,        toc_offset);
  fprintf(fp, "  toc only:          %8ld (%lx)\n", toc_only,          toc_only);
  fprintf(fp, "  toc size:          %8ld (%lx)\n", toc_size,          toc_size);
  fprintf(fp, "  toc checksum:      %016lx\n",    toc_checksum);
  fprintf(fp, "  data offset:       %8ld (%lx)\n", data_offset,       data_offset);
  fprintf(fp, "  compression:       %ld\n", compression_algorithm);
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: fprint: leaving\n", name_only(__FILE__), __LINE__);
#endif
}

/****************************************************************************/
void DPvzMetadata::fdump(FILE* fp)
/****************************************************************************/
/****************************************************************************/
{
  ssize_t size = sizeof *this;
  char* buf = (char*) this;
  const int line_len = 32;
  fprintf(fp, "DPvzMetadata (%ld)\n", size);
  for (int i=0; i < (int) size; i+=line_len) {
    fprintf(fp, "%3x-%3x:", i, min(i+line_len-1, (int) size));
    for (int j=0; j < line_len; j++) {
      if ((int) size < i+j) {
	fprintf(fp, " %2c", ' ');
      } else {
	fprintf(fp, " %02x", (int) (buf[i+j] & 0xff));
      }
    }
    fprintf(fp, "  '");
    for (int j=0; j < line_len; j++) {
      int c = buf[i+j];
      if ((int) size < i+j) {
	fprintf(fp, "%c", ' ');
      } else if (' ' <= c && c < 0x7f) {
	fprintf(fp, "%c", c);
      } else {
	fprintf(fp, ".");
      }
    }
    fprintf(fp, "'\n");
  }
}

void DPvzMetadata::set_majik(const char* m)
{
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_majik: entering majik='%s' m='%s' size=%lu\n", name_only(__FILE__), __LINE__, (majik==NULL)?"null":(const char*)majik, (m==NULL)?"null":(const char*)m, sizeof majik);
#endif
  char* n = (char*) &majik;
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_majik: memset   majik='%s' m='%s' size=%lu\n", name_only(__FILE__), __LINE__, (majik==NULL)?"null":(const char*)majik, (m==NULL)?"null":(const char*)m, sizeof majik);
#endif
  memset(n, '\0', sizeof majik);
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_majik: strncpy  majik='%s' m='%s' size=%lu\n", name_only(__FILE__), __LINE__, (majik==NULL)?"null":(const char*)majik, (m==NULL)?"null":(const char*)m, sizeof majik);
#endif
  memcpy(n, m, sizeof majik);
#if defined(DPvzTrace)
fprintf(stdout, "%s: %4d: set_majik: leaving  majik='%s' m='%s' size=%lu\n", name_only(__FILE__), __LINE__, (majik==NULL)?"null":(const char*)majik, (m==NULL)?"null":(const char*)m, sizeof majik);
#endif
}

void DPvzMetadata::set_majik(std::string m) 
{
  char* n = (char*) &majik;
  memset(n, '\0', sizeof majik);
  memcpy(n, m.c_str(), sizeof majik);
}

void DPvzMetadata::set_magic(const char* m) 
{
  char* n = (char*) &magic;
  memset(n, '\0', sizeof magic);
  memcpy(n, m, sizeof magic);
  memset(&blank, '\0', sizeof blank);
}
