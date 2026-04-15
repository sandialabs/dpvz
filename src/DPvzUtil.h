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

#if ! defined(DPVZUTIL_H)
#define DPVZUTIL_H

#include <stdio.h>
#include <string>
#include <stdbool.h>
#include <stdint.h>

uint64_t    crc64(const void* data, uint64_t size);
void        fdump(FILE* fp, std::string file_name);
void        fdump(FILE* fp, char* buf, ssize_t size);
const char* name_only(const char* name);
int64_t     pad(int64_t off, int64_t size);
double      seconds();

bool        chk_directory_data_distribution(const char* env_var_name);
bool        chk_file_data_distribution     (const char* env_var_name);
int         set_dir_lfs_stripe_size (const char* dir_name,  int64_t stripe_size, int32_t stripe_index, int32_t stripe_count);
bool        set_file_lfs_stripe_size(const char* file_name, int64_t stripe_size, int32_t stripe_index, int32_t stripe_count);
char*       find_exe(const char* exe);
char*       find_exe_in_dir(const char* exe, char* dir);

int         deflate_buf(void* tgt_buf, int tgt_max, const void* src_buf, int src_len, int compression_level);
int         inflate_buf(void* tgt_buf, int tgt_max, const void* src_buf, int src_len);
void        zerr(const char* file, int line, int err);

#if ! defined(HAVE_MEMRCHR)
void *memrchr(const void *s, int c, size_t n);
#endif

#endif
