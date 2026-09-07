/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/fault_injection.h"

int main(int argc, char **argv) {
  if (argc != 2) return 2;
  wesql::remote_commit::production_fault_point(argv[1]);
  wesql::remote_commit::production_fault_point(argv[1]);
  return 0;
}
