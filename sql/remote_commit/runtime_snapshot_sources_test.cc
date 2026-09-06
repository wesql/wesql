/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/runtime_snapshot_sources.h"
#include "sql/remote_commit/snapshot_file_classification.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
namespace rc = wesql::remote_commit;

namespace {
void expect(bool condition, const char *message) {
  if (!condition) {
    std::cerr << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

struct TemporaryDirectory {
  TemporaryDirectory() {
    std::string pattern = "/tmp/wesql-snapshot-files-XXXXXX";
    const char *created = mkdtemp(pattern.data());
    expect(created != nullptr, "cannot create fixture root");
    path = created;
  }
  ~TemporaryDirectory() {
    std::error_code error;
    fs::remove_all(path, error);
  }
  fs::path path;
};

void write_file(const fs::path &path) {
  std::ofstream output(path, std::ios::binary);
  output << "fixture bytes\n";
  output.close();
  expect(output.good(), "cannot write fixture");
}
}  // namespace

int main() {
  for (const char *path : {"auto.cnf", "mysql.ibd", "mysql_upgrade_history",
                           "mysql/proc.sdi", "sys/view.frm",
                           "performance_schema/table.sdi", "app/t_392.sdi",
                           "app/a@0020b_with_underscore_18446744073709551615.sdi"})
    expect(rc::is_mysql_dictionary_snapshot_file(path), path);
  for (const char *path : {"", "t_392.sdi", "/app/t_392.sdi",
                           "../app/t_392.sdi", "app/../t_392.sdi",
                           "./app/t_392.sdi", "app/sub/t_392.sdi",
                           "app/t.sdi", "app/_392.sdi", "app/t_.sdi",
                           "app/t_0.sdi", "app/t_01.sdi", "app/t_-1.sdi",
                           "app/t_18446744073709551616.sdi", "app/t_1x.sdi",
                           "app/t_392.SDI", "app/t_392.sdi.tmp", "app/t.ibd"})
    expect(!rc::is_mysql_dictionary_snapshot_file(path), path);

  TemporaryDirectory directory;
  fs::create_directory(directory.path / "app");
  write_file(directory.path / "mysql.ibd");
  write_file(directory.path / "ibdata1");
  write_file(directory.path / "app/t_392.sdi");
  std::vector<rc::LocalSnapshotPayload> objects;
  std::string error;
  expect(rc::finalize_runtime_snapshot_tree(
             directory.path, rc::RuntimeSnapshotTreeKind::CLONE, &objects, &error),
         error.c_str());
  expect(objects.size() == 3, "snapshot lost a persistent file");
  expect(objects.front().relative_path == "app/t_392.sdi" &&
             objects.front().component == "mysql-dd" &&
             objects.front().format == "mysql-dd-v1" &&
             objects.front().local_path == directory.path / "app/t_392.sdi",
         "SDI was not retained as an exact dictionary payload");

  const auto expect_rejected_without_output_change = [&] {
    expect(!rc::finalize_runtime_snapshot_tree(
               directory.path, rc::RuntimeSnapshotTreeKind::CLONE, &objects, &error),
           "invalid tree was accepted");
    expect(objects.size() == 3 && objects.front().relative_path == "app/t_392.sdi",
           "failed classification changed prior output");
  };
  write_file(directory.path / "app/unknown.txt");
  expect_rejected_without_output_change();
  fs::remove(directory.path / "app/unknown.txt");
  write_file(directory.path / "app/t_01.sdi");
  expect_rejected_without_output_change();
  fs::remove(directory.path / "app/t_01.sdi");
  fs::create_symlink("t_392.sdi", directory.path / "app/linked_393.sdi");
  expect_rejected_without_output_change();
  std::cout << "snapshot file classification and real tree finalization passed\n";
}
