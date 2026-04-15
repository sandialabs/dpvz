/******************************************************************************/
/*                                                                            */
/* dpvz-ar.C                                                                  */
/*                                                                            */
/* dpvz-ar options files...                                                   */
/*                                                                            */
/* # common options                                                           */
/*   -l  or --list                             show file names and sizes      */
/*   -x  or --extract                          extract files from the archive */
/*   -o  or --output       <path>              output directory or path       */
/*   -v  or --verbose                          show more details              */
/*                                                                            */
/* # less common options                                                      */
/*   -cs or --cycle-start  <cycle>             starting cycle number          */
/*   -ce or --cycle-end    <cycle>             ending cycle number            */
/*   -st or --cycle-stride <stride>            cycle stride                   */
/*   -ts or --time-start   <time>              starting simulation time       */
/*   -te or --time-end     <time>              ending simulation time         */
/*                                                                            */
/* TODO                                                                       */
/*   -n  or -name         <file>               archived file name             */
/*                                                                            */
/******************************************************************************/


#include "DPvzVtk.h"

int main(int argc, char**argv)
{
  DPvzVtk::main(argc, argv);

  return 0;
}
