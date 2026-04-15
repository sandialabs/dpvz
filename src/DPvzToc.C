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

#include "DPvzToc.h"

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <poll.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/wait.h>

#include <unistd.h>
#include <fcntl.h>


#include "DPvzUtil.h"

// Distributed (D) Parallel (P) visualization (v) compressed (z) File -- DPvzFile (.dpvz)

#define min(x,y)	(((x)<=(y)) ? (x) : (y))


/****************************************************************************/
/*                                                                          */
/*                                 DPvzToc                                  */
/*                                                                          */
/****************************************************************************/

DPvzTocEntry DPvzToc::unused_entry = {
  DPvzTocEntry::unused_ranks, 
  DPvzTocEntry::unused_cycle,
  DPvzTocEntry::unused_time,
  DPvzTocEntry::unused_toc_crc,
  DPvzTocEntry::unused_offset,
};

/****************************************************************************/
DPvzToc::DPvzToc(int fd, DPvzMetadata* meta, std::string file_name, bool& err)
/****************************************************************************/
/*                                                                          */
/* allocate a buffer for the global data. the buffer size is provided by    */
/* the metadata structure. the unpadded size is in global_only, the padded  */
/* size, which is what we allocate, is given in global_size.                */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: DPvzToc: fd=%d meta=%p, file='%s'\n", name_only(__FILE__), __LINE__, fd, meta, file_name.c_str());
#endif
  if (read(fd, meta, file_name)) {
    fprintf(stderr, "%s: %4d: DPvzToc: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    err = true;
    return;
  }
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: DPvzToc: leaving, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
#endif
}

/****************************************************************************/
DPvzTocIndex DPvzToc::get_idx(int64_t index) 
/*                                                                          */
/* this routine computes the table (L0, L1, or L2) and index(es) needed to  */
/* reach a particular time step entry in the table of contents.             */
/*                                                                          */
/****************************************************************************/
{
  int16_t table  = DPvzTocIndex::UNUSED;
  int16_t l2_idx = DPvzTocIndex::UNUSED;
  int16_t l1_idx = DPvzTocIndex::UNUSED;
  int16_t l0_idx = DPvzTocIndex::UNUSED;

  if (index < L0_min) {
    // index is less than zero
    ;
  } else if (L0_min <= index && index <= L0_max) {
    table = 0;
    int64_t idx = index - L0_min;
    l0_idx = (int16_t) ((idx & L0_mask) >> L0_shift);
  } else if (L1_min <= index && index <= L1_max) {
    table = 1;
    int64_t idx = index - L1_min;
    l0_idx = (int16_t) ((idx & L0_mask) >> L0_shift);
    l1_idx = (int16_t) ((idx & L1_mask) >> L1_shift);
  } else if (L2_min <= index && index <= L2_max) {
    table = 2;
    int64_t idx = index - L2_min;
    l0_idx = (int16_t) ((idx & L0_mask) >> L0_shift);
    l1_idx = (int16_t) ((idx & L1_mask) >> L1_shift);
    l2_idx = (int16_t) ((idx & L2_mask) >> L2_shift);
  } else {
    // index is greater than L2_max (4096^1 + 4096^2 + 4096^3)
    ;
  }

  return DPvzTocIndex(index, table, l2_idx, l1_idx, l0_idx);
}

/****************************************************************************/
int64_t DPvzToc::inv_idx(DPvzTocIndex idx)
/*                                                                          */
/* this routine inverts the index, that is, it takes the table L0, L1, and  */
/* L2 indexes needed to reach a particular time step entry in the table of  */
/* contents, and from that computes a unique index in the range of          */
/* L0_min <= idx <= L2_max. it is the inverse of the DPvzToc::get_idx()     */
/* function.                                                                */
/*                                                                          */
/****************************************************************************/
{
  int64_t result = DPvzTocIndex::UNUSED;

  if (idx.table == 0) {
    result = 
      ((idx.l0_idx << L0_shift) & L0_mask) + 
      L0_min;
  } else if (idx.table == 1) {
    result = 
      ((idx.l1_idx << L1_shift) & L1_mask) +
      ((idx.l0_idx << L0_shift) & L0_mask) +
      L1_min;
  } else if (idx.table == 2) {
    result = 
      ((idx.l2_idx << L2_shift) & L2_mask) +
      ((idx.l1_idx << L1_shift) & L1_mask) +
      ((idx.l0_idx << L0_shift) & L0_mask) +
      L2_min;
  } else {
    // table < 0 || 2 < table
    ;
  }

  return result;
}

/****************************************************************************/
bool DPvzToc::read(int fd, DPvzMetadata* meta, std::string file_name)
/*                                                                          */
/* read the table of contents (ToC). the offset for the ToC is stored in    */
/* the metadata structure as toc_offset, the size to read is in toc_only,   */
/* and the checksum is in toc_checksum.                                     */
/*                                                                          */
/* return true if there were errors, false otherwise                        */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: entering, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
#endif
  // do we have a valid file descriptor and metadata section? if not, bail...
  if (fd < 0 || meta == NULL) {
    return true;
  }

  // go to the table of contents section in the file
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: seek fd=%d offset=%ld, file='%s'\n", name_only(__FILE__), __LINE__, fd, meta->toc_offset, file_name.c_str());
#endif
  if (lseek(fd, meta->toc_offset, SEEK_SET) < 0) {
    fprintf(stderr, "%s: %4d: read: unable to seek, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    return true;
  }

  // read the table of contents section
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: read fd=%d size=%ld, file='%s'\n", name_only(__FILE__), __LINE__, fd, sizeof *this, file_name.c_str());
#endif
  if (::read(fd, this, sizeof(*this)) < (ssize_t) sizeof(*this)) {
    fprintf(stderr, "%s: %4d: read: unable to read, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("read");
    return true;
  }

  // check the table of contents checksum
  uint64_t meta_toc_checksum = meta->toc_checksum;
  uint64_t toc_checksum = crc64((const uint8_t*) this, sizeof *this);
  if (meta_toc_checksum != 0 && meta_toc_checksum != toc_checksum) {
    fprintf(stderr, "%s: %4d: read: table of contents checksums don't match, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    return true;
  }

#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: read: leaving, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
#endif

  return false;
}

/****************************************************************************/
bool DPvzToc::write(int fd, DPvzMetadata* meta, std::string file_name)
/****************************************************************************/
/*                                                                          */
/* write the table of contents (ToC). the offset for the ToC is stored in   */
/* the metadata structure as toc_offset, the size is in toc_only, and the   */
/* checksum needs to be stored in toc_checksum.                             */
/*                                                                          */
/* return true if there were errors, false otherwise                        */
/*                                                                          */
/****************************************************************************/
{
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: entering, fd=%d flags=%x meta=%p file='%s'\n", name_only(__FILE__), __LINE__, fd, fcntl(fd, F_GETFL), meta, file_name.c_str());
#endif

  // do we have a valid file descriptor and metadata section? if not, bail...
  if (fd < 0 || (fcntl(fd, F_GETFL) & O_RDWR) == 0 || meta == NULL) {
    fprintf(stderr, "%s: %4d: write: file descriptor fd=%d or metadata meta=%p is invalid, file='%s'\n", name_only(__FILE__), __LINE__, fd, meta, file_name.c_str());
    return true;
  }

  // go to the indicated offset
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: lseek fd=%d, offset=%ld, file size=%ld, file='%s'\n", name_only(__FILE__), __LINE__, fd, meta->toc_offset, lseek(fd, 0L, SEEK_END), file_name.c_str());
#endif
  if (lseek(fd, meta->toc_offset, SEEK_SET) < 0) {
    fprintf(stderr, "%s: %4d: write: unable to seek, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("lseek");
    return true;
  }

  // write the table of contents section
  size_t size = sizeof *this;
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: write fd=%d, this=%p, size=%ld, file='%s'\n", name_only(__FILE__), __LINE__, fd, this, size, file_name.c_str());
#endif
  if (::write(fd, (const char*) this, size) < (ssize_t) size) {
    fprintf(stderr, "%s: %4d: write: unable to write, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
    perror("write");
    return true;
  }
  fsync(fd);

  // generate the table of contents checksum
#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: crc64 this=%p size=%ld, file='%s'\n", name_only(__FILE__), __LINE__, this, sizeof *this, file_name.c_str());
#endif

  meta->toc_checksum = crc64((const uint8_t*) this, sizeof *this);

#if defined(DPvzTrace)
fprintf(stdout, "%-16s: %4d: write: checksum=%lx, file='%s'\n", name_only(__FILE__), __LINE__, meta->toc_checksum, file_name.c_str());
fprintf(stdout, "%-16s: %4d: write: leaving, file='%s'\n", name_only(__FILE__), __LINE__, file_name.c_str());
#endif

  return false;
}

/****************************************************************************/
void DPvzToc::fprint(FILE* fp, int fd, DPvzMetadata* meta)
/****************************************************************************/
/*                                                                          */
/* print the contents of the table of contents to the file pointer fp.      */
/*                                                                          */
/****************************************************************************/
{
  DPvzTocEntry disk_L0[table_len];
  DPvzTocEntry disk_L1[table_len];

  fprintf(fp, "DPvzToc (%ld)\n", meta->active_entries);

  for (int64_t i=L0_min; i <= min(((int64_t)meta->active_entries-1), L0_max); i++) {
    DPvzTocIndex idx = get_idx(i);
    fprintf(fp, "  [%4ld]: L%d[%4d]={%d,%d,%f,%016lx,%ld/0x%lx}\n", i, idx.table, idx.l0_idx, L0[idx.l0_idx].ranks, L0[idx.l0_idx].cycle, L0[idx.l0_idx].time, L0[idx.l0_idx].toc_crc, L0[idx.l0_idx].offset, L0[idx.l0_idx].offset);
  }

  for (int64_t i=L1_min; i <= min(((int64_t)meta->active_entries-1), L1_max); i++) {
    DPvzTocIndex idx = get_idx(i);
    if (idx.l0_idx == 0) {
      fprintf(fp, "  [%4ld]: L%d[%4d]={%d,%d,%f,%016lx,%ld/0x%lx}\n", i, idx.table, idx.l1_idx, L1[idx.l1_idx].ranks, L1[idx.l1_idx].cycle, L1[idx.l1_idx].time, L1[idx.l1_idx].toc_crc, L1[idx.l1_idx].offset, L1[idx.l1_idx].offset);
      if (lseek(fd, L1[idx.l1_idx].offset, SEEK_SET) < 0) {
	fprintf(stderr, "%s: %4d: fprint: unable to seek fd=%d\n", name_only(__FILE__), __LINE__, fd);
	perror("lseek");
	return;
      }
      if (::read(fd, &disk_L0, sizeof disk_L0) < (ssize_t) sizeof disk_L0) {
	fprintf(stderr, "%s: %4d: fprint: unable to read fd=%d\n", name_only(__FILE__), __LINE__, fd);
	perror("read");
	return;
      }
    }

    fprintf(fp, "    [%4ld]: L%d[%4d,%4d]={%d,%d,%f,%016lx,%ld}\n", i, idx.table, idx.l1_idx, idx.l0_idx, disk_L0[idx.l0_idx].ranks, disk_L0[idx.l0_idx].cycle, disk_L0[idx.l0_idx].time, disk_L0[idx.l0_idx].toc_crc, disk_L0[idx.l0_idx].offset);
  }

  for (int64_t i=L2_min; i <= min(((int64_t)meta->active_entries-1), L2_max); i++) {
    DPvzTocIndex idx = get_idx(i);
    if (idx.l0_idx == 0) {
      if (idx.l1_idx == 0) {
	fprintf(fp, "  [%4ld]: L%d[%4d]={%d,%d,%f,%lx,%ld}\n", i, idx.table, idx.l2_idx, L2[idx.l2_idx].ranks, L2[idx.l2_idx].cycle, L2[idx.l2_idx].time, L2[idx.l2_idx].toc_crc, L2[idx.l2_idx].offset);
	if (lseek(fd, L2[idx.l2_idx].offset, SEEK_SET) < 0) {
	  fprintf(stderr, "%s: %4d: fprint: unable to seek fd=%d\n", name_only(__FILE__), __LINE__, fd);
	  perror("lseek");
	  return;
	}
	if (::read(fd, &disk_L1, sizeof disk_L1) < (ssize_t) sizeof disk_L1) {
	  fprintf(stderr, "%s: %4d: fprint: unable to read fd=%d\n", name_only(__FILE__), __LINE__, fd);
	  perror("read");
	  return;
	}
      }

      fprintf(fp, "    [%4ld]: L%d[%4d,%4d]={%d,%d,%f,%lx,%ld}\n", i, idx.table, idx.l2_idx, idx.l1_idx, disk_L1[idx.l0_idx].ranks, disk_L1[idx.l0_idx].cycle, disk_L1[idx.l0_idx].time, disk_L1[idx.l0_idx].toc_crc, disk_L1[idx.l0_idx].offset);
      if (lseek(fd, disk_L1[idx.l1_idx].offset, SEEK_SET) < 0) {
	fprintf(stderr, "%s: %4d: fprint: unable to read fd=%d\n", name_only(__FILE__), __LINE__, fd);
	perror("read");
	return;
      }
      if (::read(fd, &disk_L0, sizeof disk_L0) < (ssize_t) sizeof disk_L0) {
	fprintf(stderr, "%s: %4d: fprint: unable to read fd=%d\n", name_only(__FILE__), __LINE__, fd);
	perror("read");
	return;
      }
    }

    fprintf(fp, "      [%4ld]: L%d[%4d,%4d,%4d]={%d,%d,%f,%lx,%ld}\n", i, idx.table, idx.l2_idx, idx.l1_idx, idx.l0_idx, disk_L0[idx.l0_idx].ranks, disk_L0[idx.l0_idx].cycle, disk_L0[idx.l0_idx].time, disk_L0[idx.l0_idx].toc_crc, disk_L0[idx.l0_idx].offset);
  }
}
