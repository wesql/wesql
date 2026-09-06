/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef WESQL_REMOTE_COMMIT_FAULT_INJECTION_H
#define WESQL_REMOTE_COMMIT_FAULT_INJECTION_H

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace wesql::remote_commit {

// Opt-in, one-shot fault controls for the release binary. The operator owns
// this separate directory; neither SQL nor remote objects can arm a point.
inline void production_fault_point(const char *point) {
  static const std::string directory = [] {
    const char *value = std::getenv("WESQL_REMOTE_COMMIT_FAULT_DIR");
    return value == nullptr ? std::string{} : std::string(value);
  }();
  if (directory.empty()) return;
  const std::string stem = directory + "/" + point;
  const std::string arm = stem + ".arm";
  const int input = open(arm.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (input < 0) {
    if (errno == ENOENT) return;
    _exit(86);
  }
  struct stat metadata {};
  char bytes[16];
  const ssize_t length = read(input, bytes, sizeof(bytes));
  const bool regular = fstat(input, &metadata) == 0 && S_ISREG(metadata.st_mode);
  close(input);
  if (!regular || length <= 0 ||
      length == static_cast<ssize_t>(sizeof(bytes))) _exit(86);
  const std::string action(bytes, static_cast<size_t>(length));
  if (action != "pause\n" && action != "crash\n") _exit(86);

  // A hard link claims the arm atomically without overwriting a prior claim.
  // Each point may fire once per directory, even with concurrent committers.
  if (link(arm.c_str(), (stem + ".claimed").c_str()) != 0) {
    if (errno == EEXIST || errno == ENOENT) return;
    _exit(86);
  }
  if (unlink(arm.c_str()) != 0) _exit(86);
  const int output = open((stem + ".hit").c_str(),
                          O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                          0600);
  if (output < 0) _exit(86);
  const std::string record = "{\"point\":\"" + std::string(point) +
      "\",\"pid\":" + std::to_string(getpid()) + " ,\"action\":\"" +
      action.substr(0, action.size() - 1) + "\"}\n";
  const bool recorded = write(output, record.data(), record.size()) ==
                            static_cast<ssize_t>(record.size()) &&
                        fsync(output) == 0;
  close(output);
  if (!recorded) _exit(86);
  if (action == "crash\n") _exit(86);

  // Keep other SQL threads available for visibility probes at the paused
  // boundary. The harness may instead kill this process after the hit.
  while (access((stem + ".release").c_str(), F_OK) != 0) {
    if (errno != ENOENT) _exit(86);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
}

}  // namespace wesql::remote_commit

#endif  // WESQL_REMOTE_COMMIT_FAULT_INJECTION_H
