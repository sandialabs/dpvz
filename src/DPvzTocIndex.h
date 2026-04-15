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

#if !defined(DPVZTOCINDEX_H)
#define DPVZTOCINDEX_H

#include <string>
#include <string.h>
#include <strings.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include <limits.h>
#include <math.h>

#include "zlib.h"


/****************************************************************************/
/*                                                                          */
/*                               DPvzTocIndex                               */
/*                                                                          */
/* this structure maps an index to a triple that is used to locate on disk  */
/* the contents associated with that index. an index is an integer in the   */
/* range 0 <= index <= max, where max = 2^12 + 2^24 + 2^36 - 1. the index   */
/* represents a contiguous space of time steps, whereas the triple gives    */
/* a way to locate the time step on disk through the table of contents.     */
/* the table of contents consists of three separate tables, named L0, L1,   */
/* and L2.                                                                  */
/*                                                                          */
/* L0 gives the fastest access and represents the least time steps. indexes */
/* within this range identify the time step directly.                       */
/*                                                                          */
/* L1 makes use of one level of indirection. indexes within this range use  */
/* two values. the first is an entry into the top level of the L1. the top  */
/* level is an array of pointers to a second set of tables. the top index   */
/* points to a second table, and the second index points to an entry within */
/* that table. it is the entry in the second table that points to the place */
/* on disk where the time step is located.                                  */
/*                                                                          */
/* in a similar fashion, the L2 makes use of two levels of indirection. as  */
/* before, the top level index points to an entry in the top level of the   */
/* L2. the second index points to a second level of table, but unlike the   */
/* L1, this second entry points to a third table. the third index is used   */
/* to identify the location on disk of the time step data.                  */
/*                                                                          */
/* readers who are familiar with file system i-nodes or virtual memory page */
/* tables will recognize this structure as being similar in some ways to    */
/* both.                                                                    */
/*                                                                          */
/* an additional value, the table number, is also included to identify      */
/* which of the three tables (L0, L1, or L2) contains the index.            */
/*                                                                          */
/****************************************************************************/

/* DPvz Table of Contents Index
 *
 * Table of Contents (ToC or toc) is organized into three levels.
 * L0 contains 4,096 entries, each of which points directly to a time step
 * L1 contains 4,096 entries, each of which points to a table of 4,096 entries, each of which points to a time step
 * L2 contains 4,096 entries, which point to a table of 4,096 entries, which point to a table of 4,096 entries, which point to a time step
 *
 * The total number of time steps represented is:
 *   L0 = 4,096                 = 4,096^1 = 2^12 =  4 x 2^10, or about  4 thousand
 *   L1 = 4,096 x 4,096         = 4,096^2 = 2^24 = 16 x 2^20, or about 16 million
 *   L2 = 4,096 x 4,096 x 4,096 = 4,096^3 = 2^36 = 64 x 2^30, or about 64 billion
 *
 * The space required for each of the L0, L1, and L2 tables is:
 *   4 values x 8 bytes/value x 4,096 entries per table = 128 KiB per table,
 *   or 3 x 128 Kib = 384 KiB for all three tables.
 */
class DPvzTocIndex {
public:
  static const int16_t UNUSED = -1;

  // create an unused index
  DPvzTocIndex() : index(DPvzTocIndex::UNUSED), table(DPvzTocIndex::UNUSED),
  l2_idx(DPvzTocIndex::UNUSED), l1_idx(DPvzTocIndex::UNUSED), l0_idx(DPvzTocIndex::UNUSED)
  {
  }

  // general interface, which can be used to create any index
  DPvzTocIndex(int64_t idx, int16_t tbl, int16_t l2, int16_t l1, int16_t l0)
  : index(idx), table(tbl), l2_idx(l2), l1_idx(l1), l0_idx(l0)
  {
  }

  // this interface is used as a convenience to create L2 indexes
  DPvzTocIndex(int64_t idx, int16_t l2, int16_t l1, int16_t l0)
  : index(idx), table(2), l2_idx(l2), l1_idx(l1), l0_idx(l0)
  {
  }

  // this interface is used as a convenience to create L1 indexes
  DPvzTocIndex(int64_t idx, int16_t l1, int16_t l0)
  : index(idx), table(1), l2_idx(DPvzTocIndex::UNUSED), l1_idx(l1), l0_idx(l0)
  {
  }

  // this interface is used as a convenience to create L0 indexes
  DPvzTocIndex(int64_t idx, int16_t l0)
  : index(idx), table(0), l2_idx(DPvzTocIndex::UNUSED), l1_idx(DPvzTocIndex::UNUSED), l0_idx(l0)
  {
  }

  ~DPvzTocIndex() {}

  int64_t index;			// index of this location
  int16_t table;			// level 0 (0), level 1 (1), or level 2 (2) table
  int16_t l2_idx;			// level 2 index (0 - 4095)
  int16_t l1_idx;			// level 1 index (0 - 4095)
  int16_t l0_idx;			// level 0 index (0 - 4095)
private:
};

#endif
