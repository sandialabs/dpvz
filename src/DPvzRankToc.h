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

#if !defined(DPVZRANKTOC_H)
#define DPVZRANKTOC_H

#include <stdint.h>

/*
 * a time step is described by a DPvzTocEntry, stored in the ToC, which contains:
 *   int32_t ranks;
 *   int32_t cycle;
 *   double  time;
 *   int64_t size;
 *   int64_t offset;
 *
 * time steps have their own table of contents, one entry per rank, that describe
 * the size and offset of each rank's data. the time step table of contents is a
 * DPvzRankToc. the layout of DPvzRankToc is:
 *   int64_t inflated_size;
 *   int64_t deflated_size;
 *   int64_t deflated_crc;
 *   int64_t offset;
 *
 * the layout of a time step in the file is:
 *   DPvzRankToc[ranks]		// table of contents
 *   char data[size0]		// data for rank 0 (padded)
 *   char data[size1]		// data for rank 1 (padded)
 *   ...
 *   char data[sizeK]		// data for rank K=N-1, for N ranks (padded)
 *
 * notes:
 * (1) if the padded size is needed, it can be calculated from the unpadded data size
 *     and the page size, as page_size * ((data_size + page_size - 1) / page_size)
 *
 * (2) a time step, specifically its table of contents, always begins on a page boundary.
 *
 * (3) rank data for rank 0 begins immediately following the table of contents, and may
 *     not fall on a page boundary. this is because rank 0 is expected to be the writer
 *     of the table of contents as well as its own data.
 *
 * (4) all other ranks, i.e., those ranks who are not rank 0, have data allocated to 
 *     begin on a page boundary.
 *
 * (5) data for the final rank, i.e., rank N-1, is padded up to a page boundary in order
 *     that the next time step begins on a page boundary.
 *
 * (6) data is padded to page boundaries in order to allow data to be written to the file
 *     in parallel without false data sharing conflicts.
 */

class DPvzRankToc {
public:
  DPvzRankToc();
  DPvzRankToc(int64_t inflated, int64_t deflated, uint64_t crc, int64_t off);
  ~DPvzRankToc();

  int operator==(DPvzRankToc x);

  static DPvzRankToc unused;

  int64_t  inflated_size;	// inflated (uncompressed) data size
  int64_t  deflated_size;	// deflated (compressed), unpadded data size
  uint64_t deflated_crc;	// checksum of deflated (compressed) data
  int64_t  offset;		// starting offset of rank data
};

#endif
