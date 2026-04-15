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

#if !defined(DPVZGLOBAL_H)
#define DPVZGLOBAL_H

#include "DPvzMetadata.h"

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
/*                                DPvzGlobal                                */
/*                                                                          */
/* this structure is an unformatted collection of bytes. the size, both     */
/* padded and unpadded, are stored in the metadata section, as are the      */
/* location (disk offset) and data integrity checksum.                      */
/*                                                                          */
/****************************************************************************/

class DPvzGlobal {
public:
  DPvzGlobal(DPvzMetadata* meta);
  DPvzGlobal(int fd, DPvzMetadata* meta, std::string file_name, bool& err);
  ~DPvzGlobal();
  bool read (int fd, DPvzMetadata* meta, std::string file_name);
  bool write(int fd, DPvzMetadata* meta, std::string file_name);
  bool set_data(DPvzMetadata* meta, void* buf, ssize_t size, off_t offset);

  uint8_t* data;

  void fprint(FILE* fp, DPvzMetadata* meta);
private:
};

#endif
