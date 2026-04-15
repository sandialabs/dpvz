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

#include "DPvzUtil.h"

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <poll.h>
#include <dirent.h>
#include <fcntl.h>
#include <time.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <zlib.h>

#define max(x,y)	(((y)<=(x)) ? (x) : (y))
#define min(x,y)	(((x)<=(y)) ? (x) : (y))


extern char **environ;
extern char *strdup(const char *s);


static const uint64_t ref_table[256] = {
    0x0000000000000000L, 0xB32E4CBE03A75F6FL, 0xF4843657A840A05BL, 0x47AA7AE9ABE7FF34L,
    0x7BD0C384FF8F5E33L, 0xC8FE8F3AFC28015CL, 0x8F54F5D357CFFE68L, 0x3C7AB96D5468A107L,
    0xF7A18709FF1EBC66L, 0x448FCBB7FCB9E309L, 0x0325B15E575E1C3DL, 0xB00BFDE054F94352L,
    0x8C71448D0091E255L, 0x3F5F08330336BD3AL, 0x78F572DAA8D1420EL, 0xCBDB3E64AB761D61L,
    0x7D9BA13851336649L, 0xCEB5ED8652943926L, 0x891F976FF973C612L, 0x3A31DBD1FAD4997DL,
    0x064B62BCAEBC387AL, 0xB5652E02AD1B6715L, 0xF2CF54EB06FC9821L, 0x41E11855055BC74EL,
    0x8A3A2631AE2DDA2FL, 0x39146A8FAD8A8540L, 0x7EBE1066066D7A74L, 0xCD905CD805CA251BL,
    0xF1EAE5B551A2841CL, 0x42C4A90B5205DB73L, 0x056ED3E2F9E22447L, 0xB6409F5CFA457B28L,
    0xFB374270A266CC92L, 0x48190ECEA1C193FDL, 0x0FB374270A266CC9L, 0xBC9D3899098133A6L,
    0x80E781F45DE992A1L, 0x33C9CD4A5E4ECDCEL, 0x7463B7A3F5A932FAL, 0xC74DFB1DF60E6D95L,
    0x0C96C5795D7870F4L, 0xBFB889C75EDF2F9BL, 0xF812F32EF538D0AFL, 0x4B3CBF90F69F8FC0L,
    0x774606FDA2F72EC7L, 0xC4684A43A15071A8L, 0x83C230AA0AB78E9CL, 0x30EC7C140910D1F3L,
    0x86ACE348F355AADBL, 0x3582AFF6F0F2F5B4L, 0x7228D51F5B150A80L, 0xC10699A158B255EFL,
    0xFD7C20CC0CDAF4E8L, 0x4E526C720F7DAB87L, 0x09F8169BA49A54B3L, 0xBAD65A25A73D0BDCL,
    0x710D64410C4B16BDL, 0xC22328FF0FEC49D2L, 0x85895216A40BB6E6L, 0x36A71EA8A7ACE989L,
    0x0ADDA7C5F3C4488EL, 0xB9F3EB7BF06317E1L, 0xFE5991925B84E8D5L, 0x4D77DD2C5823B7BAL,
    0x64B62BCAEBC387A1L, 0xD7986774E864D8CEL, 0x90321D9D438327FAL, 0x231C512340247895L,
    0x1F66E84E144CD992L, 0xAC48A4F017EB86FDL, 0xEBE2DE19BC0C79C9L, 0x58CC92A7BFAB26A6L,
    0x9317ACC314DD3BC7L, 0x2039E07D177A64A8L, 0x67939A94BC9D9B9CL, 0xD4BDD62ABF3AC4F3L,
    0xE8C76F47EB5265F4L, 0x5BE923F9E8F53A9BL, 0x1C4359104312C5AFL, 0xAF6D15AE40B59AC0L,
    0x192D8AF2BAF0E1E8L, 0xAA03C64CB957BE87L, 0xEDA9BCA512B041B3L, 0x5E87F01B11171EDCL,
    0x62FD4976457FBFDBL, 0xD1D305C846D8E0B4L, 0x96797F21ED3F1F80L, 0x2557339FEE9840EFL,
    0xEE8C0DFB45EE5D8EL, 0x5DA24145464902E1L, 0x1A083BACEDAEFDD5L, 0xA9267712EE09A2BAL,
    0x955CCE7FBA6103BDL, 0x267282C1B9C65CD2L, 0x61D8F8281221A3E6L, 0xD2F6B4961186FC89L,
    0x9F8169BA49A54B33L, 0x2CAF25044A02145CL, 0x6B055FEDE1E5EB68L, 0xD82B1353E242B407L,
    0xE451AA3EB62A1500L, 0x577FE680B58D4A6FL, 0x10D59C691E6AB55BL, 0xA3FBD0D71DCDEA34L,
    0x6820EEB3B6BBF755L, 0xDB0EA20DB51CA83AL, 0x9CA4D8E41EFB570EL, 0x2F8A945A1D5C0861L,
    0x13F02D374934A966L, 0xA0DE61894A93F609L, 0xE7741B60E174093DL, 0x545A57DEE2D35652L,
    0xE21AC88218962D7AL, 0x5134843C1B317215L, 0x169EFED5B0D68D21L, 0xA5B0B26BB371D24EL,
    0x99CA0B06E7197349L, 0x2AE447B8E4BE2C26L, 0x6D4E3D514F59D312L, 0xDE6071EF4CFE8C7DL,
    0x15BB4F8BE788911CL, 0xA6950335E42FCE73L, 0xE13F79DC4FC83147L, 0x521135624C6F6E28L,
    0x6E6B8C0F1807CF2FL, 0xDD45C0B11BA09040L, 0x9AEFBA58B0476F74L, 0x29C1F6E6B3E0301BL,
    0xC96C5795D7870F42L, 0x7A421B2BD420502DL, 0x3DE861C27FC7AF19L, 0x8EC62D7C7C60F076L,
    0xB2BC941128085171L, 0x0192D8AF2BAF0E1EL, 0x4638A2468048F12AL, 0xF516EEF883EFAE45L,
    0x3ECDD09C2899B324L, 0x8DE39C222B3EEC4BL, 0xCA49E6CB80D9137FL, 0x7967AA75837E4C10L,
    0x451D1318D716ED17L, 0xF6335FA6D4B1B278L, 0xB199254F7F564D4CL, 0x02B769F17CF11223L,
    0xB4F7F6AD86B4690BL, 0x07D9BA1385133664L, 0x4073C0FA2EF4C950L, 0xF35D8C442D53963FL,
    0xCF273529793B3738L, 0x7C0979977A9C6857L, 0x3BA3037ED17B9763L, 0x888D4FC0D2DCC80CL,
    0x435671A479AAD56DL, 0xF0783D1A7A0D8A02L, 0xB7D247F3D1EA7536L, 0x04FC0B4DD24D2A59L,
    0x3886B22086258B5EL, 0x8BA8FE9E8582D431L, 0xCC0284772E652B05L, 0x7F2CC8C92DC2746AL,
    0x325B15E575E1C3D0L, 0x8175595B76469CBFL, 0xC6DF23B2DDA1638BL, 0x75F16F0CDE063CE4L,
    0x498BD6618A6E9DE3L, 0xFAA59ADF89C9C28CL, 0xBD0FE036222E3DB8L, 0x0E21AC88218962D7L,
    0xC5FA92EC8AFF7FB6L, 0x76D4DE52895820D9L, 0x317EA4BB22BFDFEDL, 0x8250E80521188082L,
    0xBE2A516875702185L, 0x0D041DD676D77EEAL, 0x4AAE673FDD3081DEL, 0xF9802B81DE97DEB1L,
    0x4FC0B4DD24D2A599L, 0xFCEEF8632775FAF6L, 0xBB44828A8C9205C2L, 0x086ACE348F355AADL,
    0x34107759DB5DFBAAL, 0x873E3BE7D8FAA4C5L, 0xC094410E731D5BF1L, 0x73BA0DB070BA049EL,
    0xB86133D4DBCC19FFL, 0x0B4F7F6AD86B4690L, 0x4CE50583738CB9A4L, 0xFFCB493D702BE6CBL,
    0xC3B1F050244347CCL, 0x709FBCEE27E418A3L, 0x3735C6078C03E797L, 0x841B8AB98FA4B8F8L,
    0xADDA7C5F3C4488E3L, 0x1EF430E13FE3D78CL, 0x595E4A08940428B8L, 0xEA7006B697A377D7L,
    0xD60ABFDBC3CBD6D0L, 0x6524F365C06C89BFL, 0x228E898C6B8B768BL, 0x91A0C532682C29E4L,
    0x5A7BFB56C35A3485L, 0xE955B7E8C0FD6BEAL, 0xAEFFCD016B1A94DEL, 0x1DD181BF68BDCBB1L,
    0x21AB38D23CD56AB6L, 0x9285746C3F7235D9L, 0xD52F0E859495CAEDL, 0x6601423B97329582L,
    0xD041DD676D77EEAAL, 0x636F91D96ED0B1C5L, 0x24C5EB30C5374EF1L, 0x97EBA78EC690119EL,
    0xAB911EE392F8B099L, 0x18BF525D915FEFF6L, 0x5F1528B43AB810C2L, 0xEC3B640A391F4FADL,
    0x27E05A6E926952CCL, 0x94CE16D091CE0DA3L, 0xD3646C393A29F297L, 0x604A2087398EADF8L,
    0x5C3099EA6DE60CFFL, 0xEF1ED5546E415390L, 0xA8B4AFBDC5A6ACA4L, 0x1B9AE303C601F3CBL,
    0x56ED3E2F9E224471L, 0xE5C372919D851B1EL, 0xA26908783662E42AL, 0x114744C635C5BB45L,
    0x2D3DFDAB61AD1A42L, 0x9E13B115620A452DL, 0xD9B9CBFCC9EDBA19L, 0x6A978742CA4AE576L,
    0xA14CB926613CF817L, 0x1262F598629BA778L, 0x55C88F71C97C584CL, 0xE6E6C3CFCADB0723L,
    0xDA9C7AA29EB3A624L, 0x69B2361C9D14F94BL, 0x2E184CF536F3067FL, 0x9D36004B35545910L,
    0x2B769F17CF112238L, 0x9858D3A9CCB67D57L, 0xDFF2A94067518263L, 0x6CDCE5FE64F6DD0CL,
    0x50A65C93309E7C0BL, 0xE388102D33392364L, 0xA4226AC498DEDC50L, 0x170C267A9B79833FL,
    0xDCD7181E300F9E5EL, 0x6FF954A033A8C131L, 0x28532E49984F3E05L, 0x9B7D62F79BE8616AL,
    0xA707DB9ACF80C06DL, 0x14299724CC279F02L, 0x5383EDCD67C06036L, 0xE0ADA17364673F59L,
};

/****************************************************************************/
uint64_t crc64(const void* data, uint64_t size)
/****************************************************************************/
/*                                                                          */
/* compute the 64-bit Cyclic Redundancy Check (CRC) for the buffer.         */
/*                                                                          */
/* return the CRC, or zero if it is not a valid buffer.                     */
/*                                                                          */
/****************************************************************************/
{
  const uint8_t* buf = (const uint8_t *) data;
  uint64_t crc = 0;

  for (uint64_t i=0; buf != NULL && i < size; i++) {
    crc = ref_table[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
  }

  return crc;
}

/****************************************************************************/
void fdump(FILE* fp, std::string file_name)
/****************************************************************************/
/*                                                                          */
/****************************************************************************/
{
  int fd = open(file_name.c_str(), O_RDONLY);
  if (fd < 0) {
    return;
  }

  off_t size = lseek(fd, 0L, SEEK_END);
  char* buf = new char[size];
  lseek(fd, 0L, SEEK_SET);
  read(fd, buf, size);
  close(fd);

  fprintf(fp, "*** FILE %s *** (%ld)\n", file_name.c_str(), size);

  fdump(fp, buf, size);

  delete [] buf;
}

/****************************************************************************/
static void fdumpster(
  FILE*   fp, 		// file pointer to output device
  ssize_t start, 	// start offset of the line to be output
  ssize_t end, 		// end offset of the line to be output
  ssize_t size, 	// maximum offset to be output
  char*   buf, 		// line to be output
  ssize_t len, 		// number of characters in this line
  ssize_t line_len)	// number of characters in the longest line
/****************************************************************************/
/*                                                                          */
/* write a single line of output to fp, from buf[0] to buf[len]. start and  */
/* end give the range of the characters in the line in a larger buffer.     */
/* size gives the size of the larger buffer, used in determining the format */
/* for start and end. len may be less than or equal to line_len, but it     */
/* should not be greater.                                                   */
/*                                                                          */
/****************************************************************************/
{
  if        (size < (1L<<(1*4))) {
    fprintf(fp, "%04lx-%04lx:", start, end);
  } else if (size < (1L<<(2*4))) {
    fprintf(fp, "%04lx-%04lx:", start, end);
  } else if (size < (1L<<(3*4))) {
    fprintf(fp, "%04lx-%04lx:", start, end);
  } else if (size < (1L<<(4*4))) {
    fprintf(fp, "%04lx-%04lx:", start, end);
  } else if (size < (1L<<(5*4))) {
    fprintf(fp, "%05lx-%05lx:", start, end);
  } else if (size < (1L<<(6*4))) {
    fprintf(fp, "%06lx-%06lx:", start, end);
  } else if (size < (1L<<(7*4))) {
    fprintf(fp, "%07lx-%07lx:", start, end);
  } else if (size < (1L<<(8*4))) {
    fprintf(fp, "%08lx-%08lx:", start, end);
  }

  for (int i=0; i < line_len; i++) {
    if ((int) i < len) {
      fprintf(fp, " %02x", (int) (buf[i] & 0xff));
    } else {
      fprintf(fp, " %2c", ' ');
    }
  }

  fprintf(fp, "  '");
  for (int i=0; i < len; i++) {
    int c = buf[i];
    if (' ' <= c && c < 0x7f) {
      fprintf(fp, "%c", c);
    } else {
      fprintf(fp, ".");
    }
  }
  fprintf(fp, "'\n");
}

/****************************************************************************/
void fdump(FILE* fp, char* buf, ssize_t size)
/****************************************************************************/
/*                                                                          */
/* dump a multi-line buffer. the length of an output line is specified by   */
/* line_len. the character range of each line is printed in hex. the data   */
/* is printed both as an array of hex values and as character values.       */
/* first and last repeated lines are printed, separated by elipses (...).   */
/*                                                                          */
/****************************************************************************/
{
  if (buf == NULL || size <= 0) {
    return;
  }

  const int line_len = 32;

  // walk through all of the lines...
  int count_same_line = 0;
  char prev_line[line_len];
  int prev_start = 0;

  for (ssize_t start=0; start < size; ) {
    ssize_t remains = size - start;
    ssize_t eff_len = min(line_len, max(0, remains));
    if (eff_len <= 0) {
      break;
    }

    if (start == 0) {
      // this is the first line of output
      fdumpster(fp, start, start+eff_len-1, size, buf+start, eff_len, line_len);
      if (eff_len == line_len) {
	memcpy(prev_line, buf+start, eff_len);
	count_same_line = 1;
	prev_start = start;
      }
    } else if (eff_len != line_len || memcmp(prev_line, buf+start, eff_len) != 0) {
      // this line DOESN'T match the previous line
      if (count_same_line == 1) {
      	// nothing to dump
	;
      } else if (count_same_line == 2) {
      	// dump the previous line
	fdumpster(fp, prev_start, start-1, size, prev_line, line_len, line_len);
      } else {
      	// dump the previous line
	fprintf(fp, "...\n");
	fdumpster(fp, prev_start, start-1, size, prev_line, line_len, line_len);
      }
      fdumpster(fp, start, start+eff_len-1, size, buf+start, eff_len, line_len);

      memcpy(prev_line, buf+start, eff_len);
      count_same_line = 1;
      prev_start = start;
    } else {
      // this line DOES match the previous line
      if (size <= (start+eff_len)) {
	// this is the last line
	if (count_same_line == 1) {
	  // nothing to dump
	  ;
	} else if (count_same_line == 2) {
	  // dump the previous line
	  fdumpster(fp, prev_start, start-1, size, prev_line, line_len, line_len);
	} else {
	  // dump elipses only
	  fprintf(fp, "...\n");
	}
	fdumpster(fp, start, start+eff_len-1, size, buf+start, eff_len, line_len);
      }
      count_same_line += 1;
      prev_start = start;
    }

    start += eff_len;
  }
}

/****************************************************************************/
const char* name_only(const char* name)
/****************************************************************************/
/*                                                                          */
/* equivalent to the libgen.h function basename, but without the risk of    */
/* modifying name.                                                          */
/*                                                                          */
/****************************************************************************/
{
  const char* n = rindex((char*) name, '/');
  if (n == NULL) {
    n = name;
  } else {
    n += 1;
  }

  return n;
}

/****************************************************************************/
int64_t pad(int64_t off, int64_t size)
/****************************************************************************/
/*                                                                          */
/* round 'off' up to the next integer multiple of 'size'.                   */
/*                                                                          */
/****************************************************************************/
{
  return size * ((off + size - 1) / size);
}


/****************************************************************************/
static inline double rd_cgt()
/****************************************************************************/
/*                                                                          */
/* return the number of seconds since the start of the epoch using the      */
/* clock_gettime() system call.                                             */
/*                                                                          */
/****************************************************************************/
{
    struct timespec tv; 
    if (clock_gettime(CLOCK_MONOTONIC, &tv) != 0) {
	return 0;
    }

    return ((double) tv.tv_sec + (double) tv.tv_nsec * 1.e-9);
}


/****************************************************************************/
static inline double rd_gtod()
/****************************************************************************/
/*                                                                          */
/* return the number of seconds since the start of the epoch using the      */
/* gettimeofcay() system call.                                              */
/*                                                                          */
/****************************************************************************/
{
    struct timeval tv; 
    if (gettimeofday(&tv, NULL) != 0) {
	return 0;
    }

    return ((double) tv.tv_sec + (double) tv.tv_usec * 1.e-6);
}


/****************************************************************************/
double seconds()
/****************************************************************************/
/*                                                                          */
/* return the number of seconds since the start of the epoch, that is since */
/* midnight, january 1st, 1970. the value has a resolution of microseconds  */
/* and its data type is a double-precision (64-bit) floating-point value.   */
/*                                                                          */
/****************************************************************************/
{
  return rd_cgt();
}


/*****************************************************************************/
static void remove_white_space(char* str)
/*****************************************************************************/
/*                                                                           */
/* remove the white space from a NULL-terminated string                      */
/*                                                                           */
/*****************************************************************************/
{
    if (str != NULL) {
	int len = strlen(str);
	int j=0;
	for (int i=0; i < len; i++) {
	    if (' ' < str[i] && str[i] < 0x7F) {
		str[j++] = str[i];
	    }
	}
	str[j] = '\0';
    }
}


/*****************************************************************************/
bool chk_file_data_distribution(const char* env_var_name)
/*****************************************************************************/
/*                                                                           */
/* check the specified environment variable.                                 */
/*                                                                           */
/* return true if file striping is requested, false otherwise.               */
/*                                                                           */
/*****************************************************************************/
{
    bool result = false;

    char* restart = getenv(env_var_name);
    if (restart == NULL || strcasecmp(restart,"") == 0) {
	return false;
    }

    restart = strdup(restart);

    // remove white space
    remove_white_space(restart);

    // check the desensitized value
    if (strcasecmp(restart,"file") == 0 || strcasecmp(restart,"filestripe") == 0) {
	// explicit request for file striping
	result = true;
    } else if (strcasecmp(restart,"both") == 0) {
	// both are requested
	result = true;
    } else {
	// implicit request for file striping
	result = false;
    }

    free(restart);

    return result;
}

/*****************************************************************************/
bool chk_directory_data_distribution(const char* env_var_name)
/*****************************************************************************/
/*                                                                           */
/* check the specified environment variable.                                 */
/*                                                                           */
/* return true if directory striping is requested, false otherwise.          */
/*                                                                           */
/*****************************************************************************/
{
    bool result = false;

    char* restart = getenv(env_var_name);
    if (restart == NULL || strcasecmp(restart,"") == 0) {
	return false;
    }

    restart = strdup(restart);

    // remove white space
    remove_white_space(restart);

    // check the desensitized value
    if (strcasecmp(restart,"dir") == 0       || strcasecmp(restart,"dirstripe") == 0 ||
	strcasecmp(restart,"directory") == 0 || strcasecmp(restart,"directorystripe") == 0) {
	// explicit request for directory striping
	result = true;
    } else if (strcasecmp(restart,"both") == 0) {
	// both are requested
	result = true;
    } else {
	// implicit request for file striping
	result = false;
    }

    free(restart);

    return result;
}


/****************************************************************************/
int set_dir_lfs_stripe_size(const char* dir_name, int64_t stripe_size, int32_t stripe_index, int32_t stripe_count)
/****************************************************************************/
/* set the lustre stripe size for the directory                             */
/* stripe size is in bytes, index and count are in units of lustre OSTs     */
/* size must be a positive multiple of 64KB                                 */
/* index == -1 means system chooses                                         */
/* count == -1 means system uses all available OSTs                         */
/* if the lfs command cannot be found, or the directory is not on a lustre  */
/*    file system, no stripe characteristics are set                        */
/* the directory must exist prior to this call                              */
/****************************************************************************/
{
  // the directory must have a name
  if (dir_name == NULL) {
    return -1;
  }

  // stripe size must be 64 KB or larger
  if (stripe_size < (1<<16)) {
    return -1;
  }

  // stripe size must also be a multiple of 64 KB (we're rounding up)
  stripe_size = (1<<16) * ((stripe_size + (1<<16) - 1) / (1<<16));

  DIR* dirp = opendir(dir_name);
  if (dirp == NULL) {
      perror("opendir");
      return -1;
  }
  closedir(dirp);

  char* lfs_exe = find_exe("lfs");
  if (lfs_exe != NULL) {
    // the lfs command does exist on this system, so execute the command:
    // lfs setstripe --stripe_count -1 --stripe_index -1 --stripe_size <size> <dir_name>

    // set up the input and output pipes to the child process
    int lfs_in_fd[2];
    int lfs_ou_fd[2];
    if (pipe(lfs_in_fd) == -1 || pipe(lfs_ou_fd) == -1) {
      perror("pipe");
      return -1;
    }

    // fork the child process
    pid_t lfs_pid = fork();
    if (lfs_pid == -1) {
      perror("fork");
      return -1;
    }

    if (lfs_pid == 0) {
      // this code is executed by the child process
      if (0 <= dup2(lfs_in_fd[0], 0) && 0 <= dup2(lfs_ou_fd[1], 1) && 0 <= dup2(lfs_ou_fd[1], 2)) {
	// the pipes were successfully duplicated to stdin, stdout, and stderr, so exec the lfs command
	char count[32];
	sprintf(count, "%d", stripe_count);
	char index[32];
	sprintf(index, "%d", stripe_index);
	char size[32];
	sprintf(size, "%ld", stripe_size);
	const char* const lfs_arg[] = { lfs_exe, "setstripe", "--stripe_count", count, "--stripe_index", index, "--stripe_size", size, dir_name, NULL, };
	execve(lfs_exe, (char* const*) lfs_arg, environ);

	// the child failed to exec, so exit the child
	perror("execve");
	exit(EXIT_SUCCESS);
      } else {
	// the pipes failed to duplicate, so exit the child
	printf("%s: %4d: set_dir_lfs_stripe_size: failed on dup2\n", rindex(__FILE__, '/')+1, __LINE__);
	exit(EXIT_SUCCESS);
      }
    } else {
      // this code is executed by the parent process
      // int wc = write(lfs_in_fd[1], "Hello, world!\n", sizeof "Hello, world!\n");
      close(lfs_in_fd[0]);
      close(lfs_in_fd[1]);

      // read the output from the child, save the results in buf,
      // and time out after 750 milliseconds of no activity
      char buf[8192];
      int rc = 0;
      while (1) {
	struct pollfd fds[1];
	fds[0].fd = lfs_ou_fd[0];
	fds[0].events = POLLIN;
	if (0 == poll(fds, sizeof fds/sizeof fds[0], 750)) {
	  break;
	}
	int rd = read(lfs_ou_fd[0], buf+rc, (sizeof buf)-rc);
	if (0 < rd) {
	  rc += rd;
	  buf[rc] = '\0';
	} else {
	  break;
	}
      }
      int status;
      pid_t wpid = waitpid(lfs_pid, &status, 0);
      wpid += 0;	// no-op, to keep the compiler from complaining
      // write(1, buf, rc);
      // printf("%s: %4d: rc=%d exit=%d status=%d\n", rindex(__FILE__, '/')+1, __LINE__, rc, WIFEXITED(status), WEXITSTATUS(status));
    }
  }

  // we're done with lfs_exe and it was created with strdup, so free up the memory
  free(lfs_exe);

  return 0;
}

/****************************************************************************/
bool set_file_lfs_stripe_size(const char* file_name, int64_t stripe_size, int32_t stripe_index, int32_t stripe_count)
/*                                                                          */
/* set the lustre stripe size for the file                                  */
/* stripe size is in bytes, index and count are in units of lustre OSTs     */
/* size must be a positive multiple of 64KB                                 */
/* index == -1 means system chooses                                         */
/* count == -1 means system uses all available OSTs                         */
/* if the lfs command cannot be found, or the file is not on a lustre file  */
/*   system, no stripe characteristics are set                              */
/* if the file exists, it is deleted and a file of size zero is created in  */
/*    its place                                                             */
/*                                                                          */
/****************************************************************************/
{
  // file must have a name
  if (file_name == NULL) {
    return true;
  }

  // stripe size must be 64 KB or larger
  if (stripe_size < (1<<16)) {
    return true;
  }

  // stripe size must also be a multiple of 64 KB (we're rounding up)
  stripe_size = (1<<16) * ((stripe_size + (1<<16) - 1) / (1<<16));

  // get rid of the old file, to avoid conflicts
  unlink(file_name);

  char* lfs_exe = find_exe("lfs");
  if (lfs_exe != NULL) {
    // the lfs command does exist on this system, so execute the command:
    // lfs setstripe --stripe_count -1 --stripe_index -1 --stripe_size <size> <file_name>

    // set up the input and output pipes to the child process
    int lfs_in_fd[2];
    int lfs_ou_fd[2];
    if (pipe(lfs_in_fd) == -1 || pipe(lfs_ou_fd) == -1) {
      perror("pipe");
      return true;
    }

    // fork the child process
    pid_t lfs_pid = fork();
    if (lfs_pid == -1) {
      perror("fork");
      return true;
    }

    if (lfs_pid == 0) {
      // this code is executed by the child process
      if (0 <= dup2(lfs_in_fd[0], 0) && 0 <= dup2(lfs_ou_fd[1], 1) && 0 <= dup2(lfs_ou_fd[1], 2)) {
	// the pipes were successfully duplicated to stdin, stdout, and stderr, so exec the lfs command
	char count[32];
	sprintf(count, "%d", stripe_count);
	char index[32];
	sprintf(index, "%d", stripe_index);
	char size[32];
	sprintf(size, "%ld", stripe_size);
	const char* const lfs_arg[] = { lfs_exe, "setstripe", "--stripe_count", count, "--stripe_index", index, "--stripe_size", size, file_name, NULL, };
	execve(lfs_exe, (char* const*) lfs_arg, environ);

	// the child failed to exec, so exit the child
	perror("execve");
	exit(EXIT_SUCCESS);
      } else {
	// the pipes failed to duplicate, so exit the child
	printf("%s: %4d: set_file_lfs_stripe_size: failed on dup2\n", name_only(__FILE__), __LINE__);
	exit(EXIT_SUCCESS);
      }
    } else {
      // this code is executed by the parent process
      // int wc = write(lfs_in_fd[1], "Hello, world!\n", sizeof "Hello, world!\n");
      close(lfs_in_fd[0]);
      close(lfs_in_fd[1]);

      // read the output from the child, save the results in buf,
      // and time out after 750 milliseconds of no activity
      char buf[8192];
      int rc = 0;
      while (1) {
	struct pollfd fds[1];
	fds[0].fd = lfs_ou_fd[0];
	fds[0].events = POLLIN;
	if (0 == poll(fds, sizeof fds/sizeof fds[0], 750)) {
	  break;
	}
	int rd = read(lfs_ou_fd[0], buf+rc, (sizeof buf)-rc);
	if (0 < rd) {
	  rc += rd;
	  buf[rc] = '\0';
	} else {
	  break;
	}
      }
      int status;
      pid_t wpid = waitpid(lfs_pid, &status, 0);
      wpid += 0;	// no-op, to keep the compiler from complaining
      // write(1, buf, rc);
      // printf("%s: %4d: rc=%d exit=%d status=%d\n", name_only(__FILE__), __LINE__, rc, WIFEXITED(status), WEXITSTATUS(status));
    }
  }

  // we're done with lfs_exe and it was created with strdup, so free up the memory
  free(lfs_exe);

  // we don't actually know if we're on a Lustre file system...
  // if we ARE, the file WILL be created by the lfs command
  // if we are NOT, the file will NOT be created
  // this means we must create an empty file, or touch the file if it already exists
  int fd = open(file_name, O_WRONLY | O_CREAT, 0666);
  close(fd);

  return false;
}

/****************************************************************************/
char* find_exe(const char* exe)
/*                                                                          */
/* search each directory in PATH for the executable file                    */
/*                                                                          */
/****************************************************************************/
{
  char* result = NULL;

  // get the contents of the PATH environment variable
  char* path = getenv("PATH");
  if (exe != NULL && path != NULL) {
    // duplicate the contents of PATH
    int len = strlen(path)+1;
    char buf[len];
    memcpy(buf, path, len);
    buf[len-1] = '\0';

    // search each of the directories in the PATH
    char* dir = buf;
    while (dir != NULL) {
      char* colon = index(dir, ':');
      if (colon != NULL) {
	*colon = '\0';
	result = find_exe_in_dir(exe, dir);
	if (result != NULL) {
	  break;
	}
	dir = colon+1;
      } else {
	// last directory to search
	result = find_exe_in_dir(exe, dir);
	break;
      }
    }
  }

  return result;
}

/****************************************************************************/
char* find_exe_in_dir(const char* exe, char* dir)
/*                                                                          */
/* search the directory for the executable file                             */
/*                                                                          */
/****************************************************************************/
{
  char* result = NULL;

  if (exe != NULL && dir != NULL) {
    // remove trailing slash characters ('/')
    char* p = dir + strlen(dir) - 1;
    while (*p == '/') {
      *p = '\0';
      p--;
    }

    // open the directory
    DIR* dirp = opendir(dir);

    if (dirp != NULL) {
      // search the directory for the indicated executable file
      struct dirent* ent = readdir(dirp);
      while (ent != NULL) {
	if (strcmp(exe, ent->d_name) == 0) {
	  // we found the executable, save and return the full path
	  char buf[strlen(dir) + strlen(exe) + 2];
	  sprintf(buf, "%s/%s", dir, exe);
	  result = strdup(buf);
	  break;
	}
	// check the next file in the directory
	ent = readdir(dirp);
      }

      // close the directory
      closedir(dirp);
    }
  }

  return result;
}


/****************************************************************************/
int deflate_buf(void* tgt_buf, int tgt_max, const void* src_buf, int src_len, int compression_level)
/****************************************************************************/
/*                                                                          */
/* deflate (compress) the source buffer into the target buffer. the source  */
/* buffer is left unchanged.                                                */
/*                                                                          */
/****************************************************************************/
{
    z_stream strm;
    strm.zalloc    = Z_NULL;
    strm.zfree     = Z_NULL;
    strm.opaque    = Z_NULL;
    strm.avail_in  = 0;
    strm.next_in   = Z_NULL;
    strm.avail_out = 0;
    strm.next_out  = Z_NULL;
    int ret = ::deflateInit(&strm, compression_level);
    if (ret < Z_OK) {
      zerr(rindex(__FILE__, '/')+1, __LINE__, ret);
      return ret;
    }

    strm.avail_in  = src_len;
    strm.next_in   = (unsigned char *) src_buf;
    strm.avail_out = tgt_max;
    strm.next_out  = (unsigned char *) tgt_buf;
    ret = ::deflate(&strm, Z_FINISH);
    if (ret < Z_OK) {
      zerr(rindex(__FILE__, '/')+1, __LINE__, ret);
      return ret;
    }

    ret = ::deflateEnd(&strm);
    if (ret < Z_OK) {
      zerr(rindex(__FILE__, '/')+1, __LINE__, ret);
      return ret;
    }

    return tgt_max - strm.avail_out;
}

/****************************************************************************/
int inflate_buf(void* tgt_buf, int tgt_max, const void* src_buf, int src_len)
/****************************************************************************/
/*                                                                          */
/* inflate (expand) the source buffer into the target buffer. the source    */
/* buffer is left unchanged.                                                */
/*                                                                          */
/****************************************************************************/
{
    z_stream strm;

    strm.zalloc    = Z_NULL;
    strm.zfree     = Z_NULL;
    strm.opaque    = Z_NULL;
    strm.avail_in  = 0;
    strm.next_in   = Z_NULL;
    strm.avail_out = 0;
    strm.next_out  = Z_NULL;
    int ret = ::inflateInit(&strm);
    if (ret < Z_OK) {
	zerr(rindex(__FILE__, '/')+1, __LINE__, ret);
        return ret;
    }
 
    strm.avail_in  = src_len;
    strm.next_in   = (unsigned char *) src_buf;
    strm.avail_out = tgt_max;
    strm.next_out  = (unsigned char *) tgt_buf;
    ret = ::inflate(&strm, Z_FINISH);
    if (ret < Z_OK) {
	zerr(rindex(__FILE__, '/')+1, __LINE__, ret);
        return ret;
    }

    ret = ::inflateEnd(&strm);
    if (ret < Z_OK) {
	zerr(rindex(__FILE__, '/')+1, __LINE__, ret);
        return ret;
    }

    return tgt_max - strm.avail_out;
}

/****************************************************************************/
void zerr(const char* file, int line, int err)
/****************************************************************************/
/*                                                                          */
/****************************************************************************/
{
    if (err < 0) {
	fprintf(stderr, "%s: %4d: err=%d ", file, line, err);
	switch (err) {
	case Z_ERRNO:
	    fprintf(stderr, "error in zlib\n");
	    break;
	case Z_STREAM_ERROR:
	    fprintf(stderr, "invalid compression level\n");
	    break;
	case Z_DATA_ERROR:
	    fprintf(stderr, "invalid or incomplete deflate data\n");
	    break;
	case Z_MEM_ERROR:
	    fprintf(stderr, "out of memory\n");
	    break;
	case Z_BUF_ERROR:
	    fprintf(stderr, "buffer error\n");
	    break;
	case Z_VERSION_ERROR:
	    fprintf(stderr, "zlib version mismatch\n");
	    break;
	default:
	    fprintf(stderr, "zlib error\n");
	    break;
	}
    } else if (0 < err) {
	fprintf(stderr, "%s: %4d: err=%d\n", file, line, err);
    }
}
