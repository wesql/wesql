#!/bin/bash
#
# Script for Dev's daily work.  It is a good idea to use the exact same
# build options as the released version.

get_key_value()
{
  echo "$1" | sed 's/^--[a-zA-Z_-]*=//'
}

usage()
{
cat <<EOF
Usage: $0 [-t debug|release] [-d <dest_dir>] [-g asan|tsan]
       Or
       $0 [-h | --help]
  -t                      Select the build type.
  -d                      Set the destination directory.
  -g                      Enable the sanitizer of compiler, asan for AddressSanitizer, tsan for ThreadSanitizer
  -h, --help              Show this help message.
Note: this script is intended for internal use by MySQL developers.
EOF
}

parse_options()
{
  while test $# -gt 0
  do
    case "$1" in
    clean)
      do_clean=1;;
    -t=*)
      build_type=`get_key_value "$1"`;;
    -t)
      shift
      build_type=`get_key_value "$1"`;;
    -cc=*)
      compiler=`get_key_value "$1"`;;
    -cc)
      shift
      compiler=`get_key_value "$1"`;;
    -cxx=*)
      cxx_compiler=`get_key_value "$1"`;;
    -cxx)
      shift
      cxx_compiler=`get_key_value "$1"`;;
    -d=*)
      dest_dir=`get_key_value "$1"`;;
    -d)
      shift
      dest_dir=`get_key_value "$1"`;;
    -g=*)
      san_type=`get_key_value "$1"`;;
    -g)
      shift
      san_type=`get_key_value "$1"`;;
    -h | --help)
      usage
      exit 0;;
    *)
      echo "Unknown option '$1'"
      exit 1;;
    esac
    shift
  done
}

dump_options()
{
  echo "Dumping the options used by $0 ..."
  echo "build_type=$build_type"
  echo "dest_dir=$dest_dir"
  echo "Sanitizer=$san_type"
}

do_clean=0
build_type="debug"
dest_dir="/u01/smartengine"
san_type=""
asan=0
tsan=0

parse_options "$@"
dump_options

if [ x"$build_type" = x"debug" ]; then
  build_type="Debug"
  debug=1
elif [ x"$build_type" = x"release" ]; then
  # build_type="Release"
  build_type="RelWithDebInfo"
  debug=0
else
  echo "Invalid build type, it must be \"debug\" or \"release\"."
  exit 1
fi

if [ x"$build_type" = x"RelWithDebInfo" ]; then
  COMMON_FLAGS="-O3 -g -fexceptions -fno-omit-frame-pointer -fno-strict-aliasing -D_GLIBCXX_USE_CXX11_ABI=0 -Wno-error=deprecated-copy -Wno-error=maybe-uninitialized  -Wno-error=unused-function"
  CFLAGS="$COMMON_FLAGS"
  CXXFLAGS="$COMMON_FLAGS"
elif [ x"$build_type" = x"Debug" ]; then
  COMMON_FLAGS="-O0 -g3 -gdwarf-2 -fexceptions -fno-omit-frame-pointer -fno-strict-aliasing -D_GLIBCXX_USE_CXX11_ABI=0  -Wno-error=format-truncation"
  CFLAGS="$COMMON_FLAGS"
  CXXFLAGS="$COMMON_FLAGS"
fi

if [ x"$san_type" = x"" ]; then
    asan=0
    tsan=0
elif [ x"$san_type" = x"asan" ]; then
    CFLAGS="${CFLAGS} -fPIC"
    CXXFLAGS="${CFLAGS} -fPIC"
    asan=1
    tsan=0
elif [ x"$san_type" = x"tsan" ]; then
    CFLAGS="${CFLAGS} -fPIC"
    CXXFLAGS="${CFLAGS} -fPIC"
    asan=0
    tsan=1
else
  echo "Invalid sanitizer type, it must be \"asan\" or \"tsan\"."
  exit 1
fi

if [ x"$compiler" != x"" ] && [ x"$cxx_compiler" != x"" ]; then
  CC=$compiler
  CXX=$cxx_compiler
else
  if [ -f /etc/redhat-release ]; then
    if grep -q "CentOS Linux release 7" /etc/redhat-release; then
      CC=/opt/rh/devtoolset-7/root/usr/bin/gcc
      CXX=/opt/rh/devtoolset-7/root/usr/bin/g++
    elif grep -q "CentOS Linux release 8" /etc/redhat-release; then
      CC=/opt/rh/gcc-toolset-11/root/usr/bin/gcc
      CXX=/opt/rh/gcc-toolset-11/root/usr/bin/g++
    fi
  fi
fi

if [ ! -f $CC ] || [ ! -f $CXX ]; then
  CC=`which gcc`
  CXX=`which g++`
fi
#CC=/opt/rh/gcc-toolset-11/root/usr/bin/gcc
#CXX=/opt/rh/gcc-toolset-11/root/usr/bin/g++

export CC CFLAGS CXX CXXFLAGS

D=bu-${build_type}
[ ! -d ${D} ] && mkdir ${D}

if [[ 1 -eq $do_clean ]]; then
    echo "Cleaning ..."
    rm -f ${D}/CMakeCache.txt
    exit
fi

cd ${D} && \
rm -rf CMakeCache.txt && \
cmake .. -DCMAKE_BUILD_TYPE="$build_type"   \
         -DCMAKE_INSTALL_PREFIX="$dest_dir" \
         -DWITH_ZLIB=bundled                \
         -DWITH_ZSTD=bundled                \
         -DWITH_ASAN=$asan                  \
         -DWITH_TSAN=$tsan
#         -DWITH_TBB=ON                      \

make -j `cat /proc/cpuinfo | grep processor| wc -l`
# end of file
