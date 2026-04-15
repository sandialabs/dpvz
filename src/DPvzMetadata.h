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

#if !defined(DPVZMETADATA_H)
#define DPVZMETADATA_H

#include "DPvzTocEntry.h"
#include "DPvzToc.h"

#include <string>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

#include "zlib.h"

/****************************************************************************/
/*                                                                          */
/*                               DPvzMetadata                               */
/*                                                                          */
/* this structure tracks the relevant data needed to interpret a file. it   */
/* maintains each of the following:                                         */
/*                                                                          */
/* 1) a primary magic string to identify the file as a DPvzFile             */
/*                                                                          */
/* 2) a secondary magic string to identify the format of the time step      */
/*                                                                          */
/* 3) fields for the byte order and software version                        */
/*                                                                          */ 
/* 4) page size (shows padding to avoid false sharing in parallel writes)   */
/*                                                                          */ 
/* 5) total file size (provides a quick integrity check for the file)       */
/*                                                                          */ 
/* 6) values for offset, padded and unpadded sizes, and data integrity      */
/*    checksum, for metadata, table of contents, and global data sections   */
/*                                                                          */
/* 7) file offset to the first time step                                    */
/*                                                                          */
/* 8) algorithm used to compress time step data                             */
/*                                                                          */
/****************************************************************************/

#define DPVZ_MAJOR	1
#define DPVZ_MINOR	0
#define DPVZ_BUILD	0
#define DPVZ_REVISION	0
#define DPVZ_PAGE_SIZE	(4*1024)

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define DPVZ_BYTE_ORDER	0x0000000000000000L
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define DPVZ_BYTE_ORDER	0xFFFFFFFFFFFFFFFFL
#else
#error "unknown architecture byte order."
#endif

enum CompressionAlgorithm { NONE=Z_NO_COMPRESSION, ZLIB_FAST=Z_BEST_SPEED, ZLIB_SMALL=Z_BEST_COMPRESSION, 
  ZLIB_0=0, ZLIB_1=1, ZLIB_2=2, ZLIB_3=3, ZLIB_4=4, ZLIB_5=5, ZLIB_6=6, ZLIB_7=7, ZLIB_8=8, ZLIB_9=9, 
};

class DPvzMetadata {
public:
  DPvzMetadata(uint64_t global_size_in_bytes, const char* mj);
  DPvzMetadata(int fd, std::string file_name, bool& err);
  ~DPvzMetadata();
  bool read (int fd, std::string file_name);
  bool write(int fd, std::string file_name);

  uint8_t  magic[8];			// file identifier (magic number), "DPvzFile"
  uint8_t  blank[8];			// 0x0000000000000000
  uint8_t  majik[16];			// secondary magic number, describes the format of a timestep
  uint64_t byte_order;			// 0 -> little endian, otherwise big endian
  union {
    struct {
      uint16_t major;
      uint16_t minor;
      uint16_t build;
      uint16_t revision;
    };
    uint64_t version;
  };
  uint64_t page_size;			// page size in bytes (usually 64KiB)
  uint64_t total_file_size;		// total file size in bytes

  int64_t active_entries;		// number of active time steps in the table of contents
  DPvzTocEntry last_entry;		// copy of the last entry in the table of contents

  static const uint64_t metadata_offset = 0L;	// start of the metadata section
  uint64_t metadata_only;		// size in bytes of metadata section
  uint64_t metadata_size;		// size in bytes of metadata plus padding (64 KiB)
  uint64_t metadata_checksum;		// metadata section checksum

  uint64_t toc_offset;			// start of the table of contents (0L)
  uint64_t toc_only;			// size in bytes of table of contents
  uint64_t toc_size;			// size in bytes of table of contents plus padding (64 KiB)
  uint64_t toc_checksum;		// table of contents checksum

  uint64_t global_offset;		// start of the metadata section (0L)
  uint64_t global_only;			// size in bytes of global section
  uint64_t global_size;			// size in bytes of global plus padding (64 KiB)
  uint64_t global_checksum;		// metadata section checksum

  uint64_t data_offset;			// starting location of all time step data

  uint64_t compression_algorithm;	// compression algorithm (0=none, 1-9=libz)

  void set_majik(const char* m);
  void set_majik(std::string m);
  void set_magic(const char* m);

  void init(uint64_t global_size_in_bytes);
  void fprint(FILE* fp);
  void fdump(FILE* fp);
private:
};

#endif
