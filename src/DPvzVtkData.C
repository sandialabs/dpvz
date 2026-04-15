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

#include "DPvzVtkData.h"

#include <dirent.h>
#include <float.h>
#include <limits.h>
#include <mpi.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>           /* Definition of AT_* constants */

#include <queue>

#include "DPvzUtil.h"

struct step_rank_pair_s {
  int32_t step;
  int32_t rank;
};

bool name_to_vtk_data(const char* name, const char* output, DPvzVtkData* elt);

DPvzVtkData::DPvzVtkData()
{
}

DPvzVtkData::~DPvzVtkData()
{
}

/******************************************************************************/
/*                                                                            */
/* extract                                                                    */
/*                                                                            */
/* from the input buffer (data, size), create a table of files contained      */
/* within the data. the table is stored in array, and the number of entries   */
/* in the table is stored in count. each entry contains the following:        */
/*                                                                            */
/*  char*   name;  // original name of the file, including directories        */
/*  char*   path;  // file directory, or "./" if name does not contain a '/'  */
/*  char*   file;  // file base name, i.e., the file without the directory    */
/*  int32_t size;  // number of bytes in the file, i.e., in the data area     */
/*  char*   data;  // contents of the file                                    */
/*                                                                            */
/******************************************************************************/
bool DPvzVtkData::extract(
  char* data,				// one buffer of the file archive
  int64_t size,				// number of bytes in the data
  int& count,				// number of files in the buffer
  DPvzVtkData*& array)	// array of files in the buffer
{
  array = NULL;
  count = 0;

  // data has the format: <FILE NAME='path/file'>\ndata</FILE NAME='path/file'>\n...
  if (0 < size && data != NULL) {
#define HEADER_KEY	"<FILE NAME='"
#define TRAILER_KEY	"</FILE NAME='"
    int header_key_len  = sizeof(HEADER_KEY)  - 1;
    int trailer_key_len = sizeof(TRAILER_KEY) - 1;

    // scan through the data to determine the count of how many files are included
    for (int i=0; i < (size - 2*header_key_len); i++) {
      if (strncmp(data+i, HEADER_KEY, header_key_len) == 0) {
	count += 1;
      }
    }

    if (0 < count) {
      // allocate the file array
      array = new DPvzVtkData[count];
      for (int i=0; i < count; i++) {
	array[i].name = NULL;
	array[i].path = NULL;
	array[i].file = NULL;
	array[i].size = 0;
	array[i].data = NULL;
      }

      // perform a second sweep to extract the files
      int idx = 0;
      for (int i=0; i < (size - 2*header_key_len); i++) {
	if (strncmp(data+i, HEADER_KEY, header_key_len) == 0) {
	  // found the start of a file header, extract the name and data
	  char* header_start  = data + i;
	  char* header_end    = (char*) memchr(header_start, '\n', size - i) + 1;
	  // int   header_length = header_end - header_start;

	  char* name_start   = header_start + header_key_len;
	  char* name_end     = (char*) memchr((void*) name_start, '\'', size - i);
	  int   name_length  = name_end - name_start;
	  array[idx].name = new char[name_length+1];
	  memset(array[idx].name, '\0', name_length+1);
	  memcpy(array[idx].name, name_start, name_length);

	  char* file_end     = name_end;
	  char* file_start   = (char*) memrchr((void*) name_start, '/', name_length);
	  file_start = (file_start == NULL) ? name_start : file_start + 1;
	  int   file_length  = file_end - file_start;
	  array[idx].file = new char[file_length+1];
	  memset(array[idx].file, '\0', file_length+1);
	  memcpy(array[idx].file, file_start, file_length);

	  char* dir_start    = name_start;
	  char* dir_end      = file_start - 1;
	  int   dir_length   = dir_end - dir_start;
	  if (dir_length <= 0) {
	    dir_start  = (char*) ".";
	    dir_length = 1;
	  }
	  array[idx].path = new char[dir_length+1];
	  memset(array[idx].path, '\0', dir_length+1);
	  memcpy(array[idx].path, dir_start, dir_length);

	  // record the start of the data
	  char* data_start = header_end;
	  char* data_end   = NULL;
	  for (int j=i; j < size; j++) {
	    if (strncmp(data+j, TRAILER_KEY, trailer_key_len) == 0) {
	      data_end = data + j;
	      break;
	    }
	  }
	  int data_length = (data_end == NULL) ? 0 : data_end - data_start;

	  char* trailer_start  = data_end;
	  char* trailer_end    = (char*) memchr(trailer_start, '\n', trailer_start - data);
	  // int   trailer_length = trailer_end - trailer_start;

	  array[idx].data = data_start;
	  array[idx].size = data_length;

	  i = trailer_end - data;
	  
	  idx += 1;
	  data[i] = '\0';
	}
      }
    }
  }

  return false;
}

/******************************************************************************/
/*                                                                            */
/* free                                                                       */
/*                                                                            */
/* deallocate an extract_o_matic structure.                                   */
/*                                                                            */
/******************************************************************************/
void DPvzVtkData::free(DPvzVtkData& elt)
{
  if (elt.name != NULL) delete [] elt.name;
  if (elt.path != NULL) delete [] elt.path;
  if (elt.file != NULL) delete [] elt.file;

  elt.name = elt.path = elt.file = elt.data = NULL;
}

/******************************************************************************/
/*                                                                            */
/* free                                                                       */
/*                                                                            */
/* deallocate an array of extract_o_matic structures.                         */
/*                                                                            */
/******************************************************************************/
void DPvzVtkData::free(int& count, DPvzVtkData*& array)
{
  if (0 < count && array != NULL) {
    for (int i=0; i < count; i++) {
      free(array[i]);
    }

    delete [] array;
    array = NULL;
    count = 0;
  }
}

extern int errno;

/******************************************************************************/
/*                                                                            */
/* create                                                                     */
/*                                                                            */
/* extract the data in elt->data to the file elt->file in directory elt->path */
/* creating all necessary directories and subdirectories that are needed.     */
/*                                                                            */
/******************************************************************************/
bool DPvzVtkData::create(DPvzVtkData* elt)
{
  if (elt == NULL || elt->name == NULL || elt->path == NULL || elt->file == NULL) {
    fprintf(stderr, "%s: %4d: cannot create a NULL file or directory\n", __FILE__, __LINE__);
    return true;
  }

  if (DPvzVtkData::mkdirs(elt->path)) {
    fprintf(stderr, "%s: %4d: unable to create path='%s'\n", __FILE__, __LINE__, elt->path);
    return true;
  }

  int len = strlen(elt->path) + strlen(elt->file) + 1;
  char copy[len+1];
  sprintf(copy, "%s/%s", elt->path, elt->file);
  int fd = open(copy, O_CREAT | O_RDWR, 0777); 
  if (fd < 0) {
    fprintf(stderr, "%s: %4d: unable to open file='%s'\n", __FILE__, __LINE__, copy);
    perror("open");
    return true;
  }

  if (write(fd, elt->data, elt->size) < elt->size) {
    fprintf(stderr, "%s: %4d: unable to write file='%s' size=%d\n", __FILE__, __LINE__, copy, elt->size);
    perror("write");
    return true;
  }

  if (close(fd) < 0) {
    fprintf(stderr, "%s: %4d: unable to close file='%s'\n", __FILE__, __LINE__, copy);
    perror("close");
    return true;
  }

  return false;
}

/******************************************************************************/
/*                                                                            */
/* name_to_vtk_data                                                           */
/*                                                                            */
/******************************************************************************/
bool DPvzVtkData::name_to_vtk_data(const char* name, const char* output, DPvzVtkData* elt)
{
  if (elt == NULL || name == NULL) {
    fprintf(stderr, "%s: %4d: cannot create a NULL file or directory\n", __FILE__, __LINE__);
    return true;
  }

  // set the original file name
  elt->name = (char*) name;
  int name_len = strlen(elt->name);

  // pre-condition the name to remove duplicate '/' characters
  char pre_name[name_len+1];
  memset(pre_name, '\0', name_len+1);
  char* p = pre_name;
  const char* q = name;
  while (*q != '\0') {
    *p = *q;
    p++;
    if (*q == '/') {
      while (*q == '/') q++;
    } else {
      q++;
    }
  }

  name_len = strlen(pre_name);

  // make sure the output contains something
  output = (output == NULL || strlen(output) == 0) ? "." : output;
  int out_len = strlen(output);

  // pre-condition the output to remove duplicate '/' characters
  char pre_out[out_len+1];
  memset(pre_out, '\0', out_len+1);
  p = pre_out;
  q = output;
  while (*q != '\0') {
    *p = *q;
    p++;
    if (*q == '/') {
      while (*q == '/') q++;
    } else {
      q++;
    }
  }

  // now remove trailing '/' characters
  char* s = pre_out + strlen(pre_out) - 1;
  while (*s == '/' && pre_out < s) {
    *s = '\0';
    s -= 1;
  }

  out_len = strlen(pre_out);

  // is the name an absolute name, i.e., begins with '/'?
  bool is_absolute  = (pre_name[0] == '/');

  // does the name contain both path and file elements, or file only?
  char* ptr = pre_name;
  while (*ptr == '/') ptr++;
  bool is_file_only = (memchr(ptr, '/', name_len) == NULL);


  if        (  is_absolute &&   is_file_only) {
    // absolute path to only the file, e.g., /file.out
    elt->path = (char*) memcpy(new char[sizeof "/"], "/", sizeof "/");
    int len = strlen(ptr);
    elt->file = (char*) memcpy(new char[len+1], ptr, len+1);
  } else if (  is_absolute && ! is_file_only) {
    // absolute path including one or more directories, e.g., /dir/file.out
    char* path = ptr-1;
    int len = strlen(ptr);
    char copy[len+1];
    memcpy(copy, path, len+1);
    char* slash = (char*) memrchr(copy, '/', len+1);
    slash[0] = '\0';
    elt->path = (char*) memcpy(new char[strlen(copy)+1], copy, strlen(copy)+1);
    elt->file = (char*) memcpy(new char[strlen(slash+1)+1], slash+1, strlen(slash+1)+1);
  } else if (! is_absolute && ! is_file_only) {
    // relative path to the file with a directory, e.g., dir/file.out
    char copy[out_len + name_len + 8];
    if (strcmp(pre_out, ".") == 0) {
      snprintf(copy, out_len + name_len + 8, "%s", pre_name);
    } else {
      snprintf(copy, out_len + name_len + 8, "%s/%s", pre_out, pre_name);
    }
    char* slash = (char*) memrchr(copy, '/', strlen(copy)+1);
    slash[0] = '\0';
    elt->path = (char*) memcpy(new char[strlen(copy)   +1], copy,    strlen(copy)   +1);
    elt->file = (char*) memcpy(new char[strlen(slash+1)+1], slash+1, strlen(slash+1)+1);
  } else if (! is_absolute && is_file_only) {
    // relative path to the file with no directory, e.g., file.out
    // TODO DMP
    elt->path = (char*) memcpy(new char[strlen(pre_out) +1], pre_out,  strlen(pre_out) +1);
    elt->file = (char*) memcpy(new char[strlen(pre_name)+1], pre_name, strlen(pre_name)+1);
  } else {
fprintf(stderr, "%s: %4d: \n", __FILE__, __LINE__);
  }

  return false;
}

/******************************************************************************/
/*                                                                            */
/* mkdirs                                                                     */
/*                                                                            */
/* create a directory path consisting of one or more directories.             */
/*                                                                            */
/******************************************************************************/
bool DPvzVtkData::mkdirs(const char* path)
{
  if (path == NULL || strlen(path) == 0) {
    fprintf(stderr, "%s: %4d: cannot create a NULL directory\n", __FILE__, __LINE__);
    return true;
  }

  // make a copy of the path name that we can modify
  int path_len = strlen(path);
  char copy[path_len+1];
  memset(copy, '\0', path_len+1);
  memcpy(copy, path, path_len+1);
  bool is_absolute  = (copy[0] == '/');

  // make directories from the top down
  for (char* p=copy+(is_absolute?1:0); p != NULL && p[0] != '\0'; p++) {
    p = index(p, '/'); 
    if (p == NULL) break;
    char tmp = p[0];
    p[0] = '\0';
    if (mkdir(copy, 0777) < 0) {
      if (errno != EEXIST) {
	fprintf(stderr, "%s: %4d: unable to create directory='%s' errno=%d\n", __FILE__, __LINE__, copy, errno);
	perror("mkdir");
	return true;
      }
    }
    p[0] = tmp;
  }

  // make the final subdirectory
  if (mkdir(copy, 0777) < 0) {
    if (errno != EEXIST) {
      fprintf(stderr, "%s: %4d: unable to create directory='%s' errno=%d\n", __FILE__, __LINE__, copy, errno);
      perror("mkdir");
      return true;
    }
  }

  return false;
}
