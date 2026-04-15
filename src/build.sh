#!/bin/bash

for i in $* ; do
    if [[ $i == *=* ]] ; then
	name="`echo $i | sed -e 's/=.*$//'`"
	value="`echo $i | sed -e 's/^.*=//'`"
	echo export $name="$value"
	export $name="$value"
    else
	name="`echo $i | sed -e 's/=.*$//'`"
	value="`echo $i | sed -e 's/^.*=//'`"
	echo export $name="$value"
	export $name="$value"
    fi
done

if [[ $TRACE = "" ]] ; then
  TRACE="fprintf"
  TRACE="//"
fi

if [[ $CCS = "" ]] ; then
  CCS=mpiCC
fi

if [[ $CCM = "" ]] ; then
  CCM=mpiCC
fi

if [[ $CFL = "" ]] ; then
  CFL="-DDPvzTrace=$TRACE -Wall -O3"
  CFL="-Wall -O3"
fi

if [[ $MODE = "" ]] ; then
  MODE=serial,parallel
  mode=( `echo ${MODE[*]} | sed -e 's/,/ /g'` )
fi

if [[ $LIBSER = "" ]] ; then
  LIBSER=libDPvzSer.so
fi

if [[ $LIBMPI = "" ]] ; then
  LIBMPI=libDPvzMpi.so
fi

if [[ $LIBDIR = "" ]] ; then
    LIBDIR=../lib64
    mkdir -p $LIBDIR
fi
export LD_LIBRARY_PATH=$LIBDIR:$LD_LIBRARY_PATH

if [[ $INCLUDEDIR = "" ]] ; then
  INCLUDEDIR=../include
fi

set +x

SRC=( DPvzErr.C DPvzFile.C DPvzGlobal.C DPvzMetadata.C DPvzMode.C DPvzRankToc.C DPvzToc.C DPvzTocEntry.C DPvzTocIndex.C DPvzUtil.C DPvzVtk.C DPvzVtkData.C )
HDR=( `echo ${SRC[*]} | sed -e 's/[.]C/.h/g'` )
OBJ=( `echo ${SRC[*]} | sed -e 's/[.]C/.o/g'` )

for m in ${mode[*]} ; do
    if [[ $m == [Ss][Ee][Rr][Ii][Aa][Ll] || $m == [Ss][Ee][Rr] ]] ; then
	CFLAGS="-DDPVZ_SERIAL $CFL"
	CC="$CCS"
	LIB=$LIBDIR/$LIBSER
    elif [[ $m == [Pp][Aa][Rr][Aa][Ll][Ll][Ee][Ll] || $m == [Mm][Pp][Ii] ]] ; then
	CFLAGS="-DDPVZ_MPI $CFL"
	CC="$CCM"
	LIB=$LIBDIR/$LIBMPI
    fi

    for i in ${SRC[*]} ; do 
	echo $CC -shared -fPIC $CFLAGS -c $i
	$CC -shared -fPIC $CFLAGS -c $i
	err=$?
	if (( $err != 0 )) ; then
	  break
	fi
    done

    if (( $err != 0 )) ; then
      break
    fi

    mkdir -p `dirname $LIB`
    echo $CC -shared -fPIC $CFLAGS -o $LIB ${OBJ[*]}
    $CC -shared -fPIC $CFLAGS -o $LIB ${OBJ[*]}
    err=$?

    /bin/rm -f ${OBJ[*]}
    echo
done

if (( $err == 0 )) ; then
    mkdir -p "${INCLUDEDIR}"
    for i in ${HDR[*]} ; do 
        cp -a "${i}" "${INCLUDEDIR}"
    done
fi

if (( $err == 0 )) ; then
  echo wc -l ${SRC[*]} ${HDR[*]}
  wc -l ${SRC[*]} ${HDR[*]}
fi

