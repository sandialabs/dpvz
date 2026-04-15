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

#if !defined(DPVZTOC_H)
#define DPVZTOC_H

#include "DPvzMetadata.h"
#include "DPvzTocEntry.h"
#include "DPvzTocIndex.h"

#include <stdbool.h>
#include <stdint.h>

/****************************************************************************/
/*                                                                          */
/*                                  DPvzToc                                 */
/*                                                                          */
/* this structure represents the table of contents for all time steps saved */
/* in this file. a table of contents consists of three separate tables,     */
/* representing different size ranges and access speeds. L0 is the smallest */
/* and fastest, L2 is the largest and most complex to access. L1 is between */
/* the two for both speed and size.                                         */
/*                                                                          */
/* entries in L0 point directly to a time step, entries in L1 point to a    */
/* secondary table whose entries point to a time step, while entries in L2  */
/* point to tables that point to tables that point to time steps.           */
/*                                                                          */
/* each table (L0, L1, and L2) has 2^12 (4,096) entries, which makes the    */
/* total number of possible time steps equal to: 4096^1 + 4096^2 + 4096^3   */
/* (or roughly 64 billion entries).                                         */
/*                                                                          */
/****************************************************************************/

class DPvzMetadata;

class DPvzToc {
public:
  static const int32_t levels = 3;		// number sub-tables, i.e., L0, L1, and L2
  static const int64_t table_len = 0x1000L;	// number of entries in each sub-table (L0, L1, and L2)
  static const int64_t L0_shift  = 0L;
  static const int64_t L0_mask   = ((table_len - 1) << L0_shift);	// 0x0000000000000FFFL;
  static const int64_t L1_shift  = 12L;
  static const int64_t L1_mask   = ((table_len - 1) << L1_shift);	// 0x0000000000FFF000L;
  static const int64_t L2_shift  = 24L;
  static const int64_t L2_mask   = ((table_len - 1) << L2_shift);	// 0x0000000FFF000000L;

  // min and max indexes...
  static const int64_t L0_min = 0;
  static const int64_t L0_max = L0_min + table_len - 1;
  static const int64_t L1_min = L0_max + 1;
  static const int64_t L1_max = L1_min + table_len * table_len - 1;
  static const int64_t L2_min = L1_max + 1;
  static const int64_t L2_max = L2_min + table_len * table_len * table_len - 1;

  static DPvzTocEntry unused_entry;

  DPvzToc()  { for (int i=0; i < levels*table_len; i++) { top[i]=unused_entry; } }
  DPvzToc(int fd, DPvzMetadata* meta, std::string file_name, bool& err);
  ~DPvzToc() {}

  DPvzTocIndex get_idx(int64_t idx);
  int64_t inv_idx(DPvzTocIndex idx);
  int64_t inv_idx(                        int16_t l0) { return inv_idx(DPvzTocIndex(DPvzTocIndex::UNUSED,         l0)); }
  int64_t inv_idx(            int16_t l1, int16_t l0) { return inv_idx(DPvzTocIndex(DPvzTocIndex::UNUSED,     l1, l0)); }
  int64_t inv_idx(int16_t l2, int16_t l1, int16_t l0) { return inv_idx(DPvzTocIndex(DPvzTocIndex::UNUSED, l2, l1, l0)); }

  bool read (int fd, DPvzMetadata* meta, std::string file_name);
  bool write(int fd, DPvzMetadata* meta, std::string file_name);


  // Note: L0, L1, and L2 are the only ToC data written to a file

  // Note: all entries should contain either a valid entry or UNUSED.
  // When truncating, released entries should be assigned a value of UNUSED.
  // I'm not yet sure that I'm able to enforce that, though.
  union {
    struct {
      struct DPvzTocEntry L0[table_len];		// level-0 table of contents
      struct DPvzTocEntry L1[table_len];		// level-1 table of contents
      struct DPvzTocEntry L2[table_len];		// level-2 table of contents
    };
    struct DPvzTocEntry top[levels*table_len];		// top level of all three tables
  };

  void fprint(FILE* fp, int fd, DPvzMetadata* meta);
private:
};

#endif
