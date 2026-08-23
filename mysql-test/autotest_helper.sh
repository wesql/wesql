#!/usr/bin/env bash

get_key_value()
{
  echo "$1" | sed 's/^--[a-zA-Z_-]*=//'
}

usage()
{
cat <<EOF
Usage: $0 [-t normal|big|all]
       Or
       $0 [-h | --help]
  -t                      regresstion test type, valid options are:
                          normal, don't run big testcases (default)
                          big, only run big testcases
                          all, run both normal and big testcases
                          tp, run all test cases with threadpool enabled

  -h, --help              Show this help message.

Note: this script is intended for internal use by MySQL developers.
EOF
}

parse_options()
{
  while test $# -gt 0
  do
    case "$1" in
    -t=*)
      test_type=`get_key_value "$1"`;;
    -t)
      shift
      test_type=`get_key_value "$1"`;;
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

init_mtr_cmd()
{
  # run all test cases by default
  test_type="all"
  parse_options "$@"

  extra_mtr_option=""
  if [ x"$test_type" = x"normal" ]; then
    extra_mtr_option=""
  elif [ x"$test_type" = x"big" ]; then
    extra_mtr_option="--only-big-test --testcase-timeout=30"
  elif [ x"$test_type" = x"all" ]; then
    extra_mtr_option="--big-test --testcase-timeout=30"
  else
    echo "Invalid test type, it must be \"normal\", \"big\" or \"all\"."
    exit 1
  fi

  # Basic options
  CMD="perl mysql-test-run.pl --parallel=8 --report-unstable-tests --sanitize"
  CMD="$CMD --timer --force --skip-ndb --nounit-tests --max-connections=1000 --max-test-fail=1000"
  CMD="$CMD $extra_mtr_option"
}


run_mtr()
{
  VAR=$2
  MTR="$1 --vardir=$VAR"
  RESULT=$3
  $MTR &>${RESULT}

  grep -q '^Completed:' ${RESULT}; not_completed=$?
  if [[ ${not_completed} == 1 ]]; then
      mv ${RESULT} ${RESULT}.incompleted
      echo -e "\033[0;31mTest run failed to complete, please check result file:\033[0m ${RESULT}.incompleted!\n"
  else
    echo "MTR report: `grep '^Completed:' ${RESULT}`"
    # since 8.0.22, mysql-test-run add a shutdown_report at the end of test run
    FAIL_CASES=$(grep "Failing test(s)" ${RESULT} | sed -e 's/Failing test(s)://g; s/sanitize_report//g; s/shutdown_report//g')
    mv ${RESULT} ${RESULT}.origin
    if [[ ! -z "${FAIL_CASES}" ]]; then
      echo "Retry failed cases: ${FAIL_CASES}"
      $MTR ${FAIL_CASES} &>${RESULT}
      grep -q '^Completed:' ${RESULT}; not_completed=$?
      if [[ ${not_completed} == 1 ]]; then
          mv ${RESULT} ${RESULT}.retry_incompleted
          echo -e "\033[0;31mTest re-run failed to complete, please check result file:\033[0m ${RESULT}.retry_incompleted!\n"
      else
        echo "MTR rerun-report: `grep '^Completed:' ${RESULT}`"
        FAIL_CASES=$(grep "Failing test(s)" ${RESULT} | sed -e 's/Failing test(s)://g; s/sanitize_report//g; s/shutdown_report//g')
        mv ${RESULT} ${RESULT}.retry
        if [[ ! -z "${FAIL_CASES}" ]]; then
          echo -e "\033[0;31mFailed cases after retry:\033[0m ${FAIL_CASES}\n"
        fi
      fi
    fi

    if [[ -z "${FAIL_CASES}" ]]; then
      /bin/rm -rf $VAR
    fi
  fi
}
