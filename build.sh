#!/bin/bash

# Script for Dev's daily work.  It is a good idea to use the exact same
# build options as the released version.

function version_ge()
{
  if test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1"
  then
      return 0
  else
      return 1
  fi
}

get_mach_type()
{
  # ARM64: aarch64
  # SW64 : sw_64
  # X86  : x86_64
  mach_type=`uname -m`;
}

get_key_value()
{
  echo "$1" | sed 's/^--[a-zA-Z_-]*=//'
}

usage()
{
cat <<EOF
Usage: $0 [-t debug|release] [-d <dest_dir>] [-s <server_suffix>] [-g asan|tsan] [-r]
       Or
       $0 [-h | --help]
  -l                      Set compiler, gcc or clang
  -t                      Select the build type.
  -d                      Set the destination directory.
  -s                      Set the server suffix.
  -g                      Enable the sanitizer of compiler, asan for AddressSanitizer, tsan for ThreadSanitizer
  -c                      Enable GCC coverage compiler option
  -r                      rebuild without make chean
  -h, --help              Show this help message.

Note: this script is intended for internal use by MySQL developers.
EOF

echo -e "The build directory in debug mode is \"bu-Debug\" and \"bu-RelWithDebInfo\" in release mode"
echo -e "The usage in daily development: \"sh build.sh -t debug -d /path-to-install\""
}

parse_options()
{
  while test $# -gt 0
  do
    case "$1" in
    -l=*)
      compiler=`get_key_value "$1"`;;
    -l)
      shift
      compiler=`get_key_value "$1"`;;
    -t=*)
      build_type=`get_key_value "$1"`;;
    -t)
      shift
      build_type=`get_key_value "$1"`;;
    -d=*)
      dest_dir=`get_key_value "$1"`;;
    -d)
      shift
      dest_dir=`get_key_value "$1"`;;
    -s=*)
      server_suffix=`get_key_value "$1"`;;
    -s)
      shift
      server_suffix=`get_key_value "$1"`;;
    -g=*)
      san_type=`get_key_value "$1"`;;
    -g)
      shift
      san_type=`get_key_value "$1"`;;
    -c=*)
      enable_gcov=`get_key_value "$1"`;;
    -c)
      shift
      enable_gcov=`get_key_value "$1"`;;
    -r)
      with_rebuild=1;;
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
  echo "compiler=$compiler"
  echo "Dumping the options used by $0 ..."
  echo "build_type=$build_type"
  echo "dest_dir=$dest_dir"
  echo "server_suffix=$server_suffix"
  echo "Sanitizer=$san_type"
  echo "GCOV=$enable_gcov"
  echo "mach_tpye=$mach_type"
  echo "cmake_version=$cmake_version"
  echo "cmake_path=$CMAKE"
  echo "compiler_version=$compiler_version"
  echo "compiler_path=$CC"
  echo "cxx_path=$CXX"
  echo "CFLAGS=$CFLAGS"
  echo "CXXFLAGS=$CXXFLAGS"
}

compiler="gcc"
build_type="debug"
dest_dir=$HOME/tmp_run
server_suffix="wesql-dev"
san_type=""
asan=0
tsan=0
enable_gcov=0
with_rebuild=0

parse_options "$@"
get_mach_type

if [ x"$compiler" = x"gcc" ]; then
  CC=gcc
  CXX=g++
elif [ x"$compiler" = x"clang" ]; then
  CC=clang
  CXX=clang++
else
  echo "Invalid comiler type, it must be \"gcc\" or \"clang\"."
  exit 1
fi


# Update choosed version
compiler_version=`$CC --version | awk 'NR==1 {print $3}'`
cmake_version=`cmake --version | awk 'NR==1 {print $3}'`


if [ x"$build_type" = x"debug" ]; then
  build_type="Debug"
  debug=1
  if [ $enable_gcov -eq 1 ]; then
    gcov=1
  else
    gcov=0
  fi
elif [ x"$build_type" = x"release" ]; then
  # Release CMAKE_BUILD_TYPE is not compatible with mysql 8.0
  # build_type="Release"
  build_type="RelWithDebInfo"
  debug=0
  gcov=0
else
  echo "Invalid build type, it must be \"debug\" or \"release\"."
  exit 1
fi

server_suffix="-""$server_suffix"

if [ x"$build_type" = x"RelWithDebInfo" ]; then
  COMMON_FLAGS="-O3 -g -fexceptions -fno-strict-aliasing"
elif [ x"$build_type" = x"Debug" ]; then
  COMMON_FLAGS="-O0 -g3 -gdwarf-4 -fexceptions -fno-strict-aliasing"
fi

if [ x"$mach_type" = x"x86_64" ]; then # X86
  COMMON_FLAGS="$COMMON_FLAGS -fno-omit-frame-pointer -D_GLIBCXX_USE_CXX11_ABI=1"
elif [ x"$mach_type" = x"aarch64" ]; then # ARM64
  # ARM64 needn't more flags
  COMMON_FLAGS="$COMMON_FLAGS" #"-static-libstdc++ -static-libgcc"
fi

COMMON_FLAGS="$COMMON_FLAGS -fdiagnostics-color=always"
export GCC_COLORS='error=01;31:warning=01;35:note=01;36:caret=01;32:locus=01:quote=01'

CFLAGS="$COMMON_FLAGS"
CXXFLAGS="$COMMON_FLAGS"

if [ x"$san_type" = x"" ]; then
    asan=0
    tsan=0
elif [ x"$san_type" = x"asan" ]; then
    asan=1
    tsan=0
    ## gcov is conflicting with gcc sanitizer (at least for devtoolset-7),
    ## disable gcov if sanitizer is requested
    gcov=0
elif [ x"$san_type" = x"tsan" ]; then
    asan=0
    tsan=1
    ## gcov is conflicting with gcc sanitizer (at least for devtoolset-7),
    ## disable gcov if sanitizer is requested
    gcov=0
else
  echo "Invalid sanitizer type, it must be \"asan\" or \"tsan\"."
  exit 1
fi

# Dumpl options
dump_options

export CC CFLAGS CXX CXXFLAGS

# build directory
BU="bu-$build_type"

if [ x"$with_rebuild" = x"1" ]; then
  echo "need rebuild without clean".
  cd $BU
else
  echo "need rebuild with clean".
  mkdir $BU
  cd $BU
  echo $BU
  # Avoid unexpected cmake rerunning
  rm -rf packaging/deb-in/CMakeFiles/progress.marks
  rm -rf CMakeCache.txt
  make clean
  cmake ..                               \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
      -DFORCE_INSOURCE_BUILD=ON          \
      -DCMAKE_BUILD_TYPE="$build_type"   \
      -DWITH_PROTOBUF:STRING=bundled     \
      -DSYSCONFDIR="$dest_dir"           \
      -DCMAKE_INSTALL_PREFIX="$dest_dir" \
      -DMYSQL_DATADIR="$dest_dir/data"   \
      -DWITH_DEBUG=$debug                \
      -DENABLE_GCOV=$gcov                \
      -DINSTALL_LAYOUT=STANDALONE        \
      -DMYSQL_MAINTAINER_MODE=0          \
      -DWITH_EMBEDDED_SERVER=0           \
      -DWITH_ZLIB=bundled                \
      -DWITH_ZSTD=bundled                \
      -DWITH_MYISAM_STORAGE_ENGINE=1     \
      -DWITH_INNOBASE_STORAGE_ENGINE=1   \
      -DWITH_CSV_STORAGE_ENGINE=1        \
      -DWITH_ARCHIVE_STORAGE_ENGINE=1    \
      -DWITH_BLACKHOLE_STORAGE_ENGINE=1  \
      -DWITH_FEDERATED_STORAGE_ENGINE=1  \
      -DWITH_PERFSCHEMA_STORAGE_ENGINE=1 \
      -DWITH_EXAMPLE_STORAGE_ENGINE=0    \
      -DWITH_TEMPTABLE_STORAGE_ENGINE=1  \
      -DWITH_SMARTENGINE_STORAGE_ENGINE=1    \
      -DWITH_QUERY_TRACE=1               \
      -DWITH_EXTRA_CHARSETS=all          \
      -DDEFAULT_CHARSET=utf8mb4          \
      -DDEFAULT_COLLATION=utf8mb4_0900_ai_ci \
      -DENABLED_PROFILING=1              \
      -DENABLED_LOCAL_INFILE=1           \
      -DWITH_ASAN=$asan                  \
      -DWITH_TSAN=$tsan                  \
      -DDOWNLOAD_BOOST=1                \
      -DWITH_BOOST="../extra/"		\
      -DMYSQL_SERVER_SUFFIX="$server_suffix"         \
      -DWITH_UNIT_TESTS=0 \
      -DWITH_CLONE=1 \
      -DWITH_JEMALLOC=1 \
      -DWITH_WESQL=1 \

fi

make -j$(nproc)

# set mtr binary directory
echo "set mtr binary directory: $PWD"
export MTR_BINDIR=$PWD
