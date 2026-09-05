/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/startup_server.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cctype>
#include <cstring>
#include <limits>
#include <string>
#include <system_error>
#include <utility>

#include <fcntl.h>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
#include <openssl/evp.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;

namespace wesql::remote_commit {
namespace {

namespace fs = std::filesystem;

constexpr std::string_view kModeOption = "--wesql-remote-startup-mode";
constexpr std::string_view kInputOption = "--wesql-remote-startup-input";
constexpr std::string_view kInputSizeOption =
    "--wesql-remote-startup-input-size";
constexpr std::string_view kInputShaOption =
    "--wesql-remote-startup-input-sha256";
constexpr std::string_view kOutputOption = "--wesql-remote-startup-output";
constexpr std::string_view kDaemonPipeOption =
    "--wesql-remote-startup-daemon-pipe-fd";
constexpr char kManifestDomain[] =
    "wesql.remote_commit.stopped_root.v1";

enum class ManagedPathRule : uint8_t {
  ABSENT,
  EMPTY,
  EMPTY_OR_DOT,
  EXACT,
};

struct ManagedPathOption {
  std::string_view name;
  ManagedPathRule rule;
  std::string_view expected;
};

constexpr std::array<ManagedPathOption, 12> kManagedPathOptions = {{
    {"innodb-data-home-dir", ManagedPathRule::EMPTY_OR_DOT, {}},
    {"innodb-data-file-path", ManagedPathRule::EXACT,
     "ibdata1:12M:autoextend"},
    {"innodb-temp-data-file-path", ManagedPathRule::EXACT,
     "ibtmp1:12M:autoextend"},
    {"innodb-log-group-home-dir", ManagedPathRule::EMPTY_OR_DOT, {}},
    {"innodb-undo-directory", ManagedPathRule::EMPTY_OR_DOT, {}},
    {"innodb-temp-tablespaces-dir", ManagedPathRule::ABSENT, {}},
    {"innodb-directories", ManagedPathRule::EMPTY, {}},
    {"innodb-doublewrite-dir", ManagedPathRule::EMPTY, {}},
    {"smartengine-data-dir", ManagedPathRule::EXACT, "smartengine"},
    {"smartengine-wal-dir", ManagedPathRule::EXACT, "smartengine"},
    {"smartengine-persistent-cache-dir", ManagedPathRule::EMPTY, {}},
    {"smartengine-persistent-cache-size", ManagedPathRule::EXACT, "0"},
}};

bool fail(std::string *error, std::string detail) {
  if (error != nullptr) *error = std::move(detail);
  return false;
}

bool managed_value_matches(const ManagedPathOption &option,
                           std::string_view value) {
  switch (option.rule) {
    case ManagedPathRule::ABSENT:
      return false;
    case ManagedPathRule::EMPTY:
      return value.empty();
    case ManagedPathRule::EMPTY_OR_DOT:
      return value.empty() || value == ".";
    case ManagedPathRule::EXACT:
      return value == option.expected;
  }
  return false;
}

bool parsed_managed_value_matches(const ManagedPathOption &option,
                                  std::string_view value) {
  return option.rule == ManagedPathRule::ABSENT
             ? value.empty()
             : managed_value_matches(option, value);
}

std::string normalized_option_name(std::string_view value) {
  std::string result(value);
  std::replace(result.begin(), result.end(), '_', '-');
  return result;
}

bool is_startup_option_separator(std::string_view argument) {
  return argument == "----args-separator----" ||
         argument == "----persist-args-separator----";
}

struct ParsedManagedOptionName {
  const ManagedPathOption *option{nullptr};
  bool negated{false};
  bool ambiguous{false};
};

enum class RemoteCommitOptionPolarity : uint8_t {
  DIRECT,
  ENABLE,
  DISABLE,
};

struct ParsedRemoteCommitOption {
  bool matched{false};
  bool has_value{false};
  bool value{false};
};

bool ascii_case_equal(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(),
                    [](unsigned char left_byte, unsigned char right_byte) {
                      return std::tolower(left_byte) ==
                             std::tolower(right_byte);
                    });
}

ParsedRemoteCommitOption parse_remote_commit_option(
    std::string_view argument) {
  constexpr std::string_view kName = "binlog-archive-remote-commit";
  if (!argument.starts_with("--")) return {};
  const size_t delimiter = argument.find('=');
  std::string name = normalized_option_name(argument.substr(
      2, delimiter == std::string_view::npos ? std::string_view::npos
                                             : delimiter - 2));
  RemoteCommitOptionPolarity polarity =
      RemoteCommitOptionPolarity::DIRECT;
  for (;;) {
    if (name.starts_with("loose-")) {
      name.erase(0, sizeof("loose-") - 1);
      continue;
    }
    if (name.starts_with("skip-")) {
      polarity = RemoteCommitOptionPolarity::DISABLE;
      name.erase(0, sizeof("skip-") - 1);
      continue;
    }
    if (name.starts_with("disable-")) {
      polarity = RemoteCommitOptionPolarity::DISABLE;
      name.erase(0, sizeof("disable-") - 1);
      continue;
    }
    if (name.starts_with("enable-")) {
      polarity = RemoteCommitOptionPolarity::ENABLE;
      name.erase(0, sizeof("enable-") - 1);
      continue;
    }
    break;
  }
  if (name != kName) return {};

  const bool inline_value = delimiter != std::string_view::npos;
  const std::string_view value =
      inline_value ? argument.substr(delimiter + 1) : std::string_view{};
  if (polarity == RemoteCommitOptionPolarity::DISABLE)
    return {true, true, inline_value && value == "0"};
  if (polarity == RemoteCommitOptionPolarity::ENABLE)
    return {true, true, !inline_value || value != "0"};
  if (!inline_value) return {true, true, true};
  if (ascii_case_equal(value, "true") || ascii_case_equal(value, "on") ||
      value == "1")
    return {true, true, true};
  if (ascii_case_equal(value, "false") || ascii_case_equal(value, "off") ||
      value == "0")
    return {true, true, false};
  // Sys_var_bool uses OPT_ARG: my_getopt warns and assigns false when an
  // explicit optional boolean value is invalid.
  return {true, true, false};
}

ParsedManagedOptionName parse_managed_option_name(std::string_view raw_name) {
  std::string name = normalized_option_name(raw_name);
  bool loose = false;
  bool plugin = false;
  bool negated = false;
  for (;;) {
    if (name.starts_with("loose-")) {
      if (loose) return {nullptr, false, true};
      loose = true;
      name.erase(0, sizeof("loose-") - 1);
      continue;
    }
    if (name.starts_with("plugin-")) {
      if (plugin) return {nullptr, false, true};
      plugin = true;
      name.erase(0, sizeof("plugin-") - 1);
      continue;
    }
    if (name.starts_with("skip-")) {
      if (negated) return {nullptr, false, true};
      negated = true;
      name.erase(0, sizeof("skip-") - 1);
      continue;
    }
    if (name.starts_with("disable-")) {
      if (negated) return {nullptr, false, true};
      negated = true;
      name.erase(0, sizeof("disable-") - 1);
      continue;
    }
    break;
  }
  constexpr std::string_view kXenginePrefix = "xengine-";
  constexpr std::string_view kSmartenginePrefix = "smartengine-";
  if (name.starts_with(kXenginePrefix)) {
    name.erase(0, kXenginePrefix.size());
    name.insert(0, kSmartenginePrefix);
  }
  for (const ManagedPathOption &option : kManagedPathOptions) {
    if (name == option.name) return {&option, negated, false};
  }
  return {};
}

std::string errno_detail(std::string_view operation) {
  std::string detail(operation);
  detail.append(": ").append(std::strerror(errno));
  return detail;
}

bool lowercase_sha256(std::string_view value) {
  return value.size() == 64 &&
         std::all_of(value.begin(), value.end(), [](const char byte) {
           return (byte >= '0' && byte <= '9') ||
                  (byte >= 'a' && byte <= 'f');
         });
}

std::string hex_digest(const unsigned char *digest, size_t size) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string result(size * 2, '0');
  for (size_t index = 0; index < size; ++index) {
    result[index * 2] = kHex[digest[index] >> 4];
    result[index * 2 + 1] = kHex[digest[index] & 0x0f];
  }
  return result;
}

bool sha256_bytes(std::string_view bytes, std::string *digest,
                  std::string *error) {
  if (digest == nullptr) return fail(error, "null SHA-256 output");
  std::array<unsigned char, EVP_MAX_MD_SIZE> output{};
  unsigned int output_size = 0;
  EVP_MD_CTX *context = EVP_MD_CTX_new();
  if (context == nullptr) return fail(error, "cannot allocate SHA-256 context");
  const bool ok = EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 1 &&
                  EVP_DigestUpdate(context, bytes.data(), bytes.size()) == 1 &&
                  EVP_DigestFinal_ex(context, output.data(), &output_size) ==
                      1 &&
                  output_size == 32;
  EVP_MD_CTX_free(context);
  if (!ok) return fail(error, "cannot calculate SHA-256");
  *digest = hex_digest(output.data(), output_size);
  return true;
}

bool write_all(int descriptor, std::string_view bytes, std::string *error) {
  size_t written = 0;
  while (written < bytes.size()) {
    const ssize_t result =
        ::write(descriptor, bytes.data() + written, bytes.size() - written);
    if (result < 0 && errno == EINTR) continue;
    if (result <= 0) return fail(error, errno_detail("cannot write proof file"));
    written += static_cast<size_t>(result);
  }
  return true;
}

bool fsync_directory(const fs::path &directory, std::string *error) {
  const int descriptor =
      ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (descriptor < 0)
    return fail(error, errno_detail("cannot open proof parent directory"));
  const int sync_result = ::fsync(descriptor);
  const int sync_errno = errno;
  const int close_result = ::close(descriptor);
  if (sync_result != 0) {
    errno = sync_errno;
    return fail(error, errno_detail("cannot fsync proof parent directory"));
  }
  if (close_result != 0)
    return fail(error, errno_detail("cannot close proof parent directory"));
  return true;
}

bool absolute_path(const fs::path &path, std::string_view authority,
                   std::string *error) {
  if (path.empty() || !path.is_absolute() || path.filename().empty()) {
    std::string detail(authority);
    detail.append(" must be an absolute file path");
    return fail(error, std::move(detail));
  }
  return true;
}

bool valid_reference(const StartupProofReference &reference,
                     uint64_t maximum_bytes, std::string *error) {
  return absolute_path(reference.path, "startup proof", error) &&
         (reference.path.lexically_normal() == reference.path
              ? true
              : fail(error, "startup proof path is not lexically canonical")) &&
         (reference.size > 0 && reference.size <= maximum_bytes
              ? true
              : fail(error, "startup proof size is outside its bound")) &&
         (lowercase_sha256(reference.sha256)
              ? true
              : fail(error, "startup proof SHA-256 is invalid"));
}

bool option_matches(std::string_view argument, std::string_view name,
                    bool *has_inline_value) {
  if (!argument.starts_with("--")) return false;
  const size_t delimiter = argument.find('=');
  std::string normalized(argument.substr(0, delimiter));
  std::replace(normalized.begin(), normalized.end(), '_', '-');
  constexpr std::string_view kLoosePrefix = "--loose-";
  if (normalized.starts_with(kLoosePrefix))
    normalized.erase(2, kLoosePrefix.size() - 2);
  if (normalized != name) return false;
  *has_inline_value = delimiter != std::string_view::npos;
  return true;
}

bool remove_option(std::string_view argument, bool isolate_runtime,
                   bool *takes_next) {
  static constexpr std::array<std::string_view, 7> kAlwaysValueOptions = {
      kModeOption,     kInputOption,  kInputSizeOption, kInputShaOption,
      kOutputOption,   kDaemonPipeOption, "--datadir"};
  for (const std::string_view option : kAlwaysValueOptions) {
    bool inline_value = false;
    if (option_matches(argument, option, &inline_value)) {
      *takes_next = !inline_value;
      return true;
    }
  }
  static constexpr std::array<std::string_view, 2> kAlwaysFlagOptions = {
      "--initialize", "--initialize-insecure"};
  for (const std::string_view option : kAlwaysFlagOptions) {
    bool inline_value = false;
    if (option_matches(argument, option, &inline_value)) {
      *takes_next = false;
      return true;
    }
  }
  static constexpr std::array<std::string_view, 9> kRuntimeValueOptions = {
      "--pid-file",      "--socket",       "--port",
      "--admin-port",    "--admin-address", "--bind-address",
      "--report-port",   "--report-host",  "--log-error"};
  if (isolate_runtime) {
    for (const std::string_view option : kRuntimeValueOptions) {
      bool inline_value = false;
      if (option_matches(argument, option, &inline_value)) {
        *takes_next = !inline_value && option != "--log-error";
        return true;
      }
    }
  }
  static constexpr std::array<std::string_view, 7> kRuntimeFlagOptions = {
      "--daemonize",           "--skip-daemonize",
      "--keep-after-daemonize", "--networking",
      "--skip-networking",     "--mysqlx", "--skip-mysqlx"};
  for (const std::string_view option : kRuntimeFlagOptions) {
    bool inline_value = false;
    if ((isolate_runtime || option == "--daemonize" ||
         option == "--skip-daemonize" ||
         option == "--keep-after-daemonize") &&
        option_matches(argument, option, &inline_value)) {
      *takes_next = false;
      return true;
    }
  }
  return false;
}

bool is_print_defaults_option(std::string_view argument) {
  bool inline_value = false;
  return option_matches(argument, "--print-defaults", &inline_value);
}

bool validate_defaults_path_option(const std::vector<std::string> &arguments,
                                   size_t index, std::string *error) {
  static constexpr std::array<std::string_view, 2> kOptions = {
      "--defaults-file", "--defaults-extra-file"};
  for (const std::string_view option : kOptions) {
    bool inline_value = false;
    if (!option_matches(arguments[index], option, &inline_value)) continue;
    std::string_view value;
    if (inline_value) {
      value = std::string_view(arguments[index]).substr(
          arguments[index].find('=') + 1);
    } else {
      if (index + 1 >= arguments.size())
        return fail(error, "defaults path option is missing its value");
      value = arguments[index + 1];
    }
    if (value.empty() || !fs::path(value).is_absolute()) {
      std::string detail("startup child requires an absolute ");
      detail.append(option);
      return fail(error, std::move(detail));
    }
  }
  return true;
}

bool append_option(std::string_view name, const fs::path &value,
                   std::vector<std::string> *arguments, std::string *error) {
  const std::string encoded = value.string();
  if (encoded.empty() || encoded.find('\0') != std::string::npos)
    return fail(error, "startup argv contains an invalid path");
  std::string option(name);
  option.push_back('=');
  option.append(encoded);
  arguments->push_back(std::move(option));
  return true;
}

std::array<unsigned char, 8> big_endian_uint64(uint64_t value) {
  std::array<unsigned char, 8> bytes{};
  for (size_t index = 0; index < bytes.size(); ++index) {
    bytes[bytes.size() - 1 - index] = static_cast<unsigned char>(value & 0xff);
    value >>= 8;
  }
  return bytes;
}

bool digest_update(EVP_MD_CTX *context, const void *bytes, size_t size) {
  return EVP_DigestUpdate(context, bytes, size) == 1;
}

bool hash_regular_file(const fs::path &path, uint64_t *size,
                       std::string *digest, std::string *error) {
  const int descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (descriptor < 0) return fail(error, errno_detail("cannot open root file"));

  struct stat before {};
  if (::fstat(descriptor, &before) != 0 || !S_ISREG(before.st_mode) ||
      before.st_size < 0) {
    const int saved_errno = errno;
    ::close(descriptor);
    errno = saved_errno;
    return fail(error, "root entry stopped being a regular file");
  }

  EVP_MD_CTX *context = EVP_MD_CTX_new();
  if (context == nullptr) {
    ::close(descriptor);
    return fail(error, "cannot allocate root-file SHA-256 context");
  }
  bool ok = EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 1;
  std::array<unsigned char, 1024 * 1024> buffer{};
  uint64_t bytes_read = 0;
  while (ok) {
    const ssize_t count = ::read(descriptor, buffer.data(), buffer.size());
    if (count < 0 && errno == EINTR) continue;
    if (count < 0) {
      ok = false;
      break;
    }
    if (count == 0) break;
    bytes_read += static_cast<uint64_t>(count);
    ok = EVP_DigestUpdate(context, buffer.data(),
                          static_cast<size_t>(count)) == 1;
  }

  struct stat after {};
  if (::fstat(descriptor, &after) != 0 || ::close(descriptor) != 0) ok = false;
  std::array<unsigned char, EVP_MAX_MD_SIZE> output{};
  unsigned int output_size = 0;
  if (ok)
    ok = EVP_DigestFinal_ex(context, output.data(), &output_size) == 1 &&
         output_size == 32;
  EVP_MD_CTX_free(context);
  if (!ok) return fail(error, errno_detail("cannot hash stable root file"));
  if (before.st_dev != after.st_dev || before.st_ino != after.st_ino ||
      before.st_size != after.st_size ||
      before.st_mtime != after.st_mtime ||
      bytes_read != static_cast<uint64_t>(before.st_size))
    return fail(error, "root file changed while it was hashed");
  *size = bytes_read;
  *digest = hex_digest(output.data(), output_size);
  return true;
}

bool valid_relative_path(const fs::path &path, std::string *encoded) {
  if (path.empty() || path.is_absolute()) return false;
  for (const fs::path &component : path) {
    if (component.empty() || component == "." || component == "..")
      return false;
  }
  *encoded = path.generic_string();
  return !encoded->empty() && encoded->find('\0') == std::string::npos;
}

bool hash_root_manifest(const StartupRootSnapshot &snapshot,
                        std::string *digest, std::string *error) {
  EVP_MD_CTX *context = EVP_MD_CTX_new();
  if (context == nullptr)
    return fail(error, "cannot allocate root-manifest SHA-256 context");
  bool ok = EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 1 &&
            digest_update(context, kManifestDomain, sizeof(kManifestDomain));
  for (const StartupRootEntry &entry : snapshot.entries) {
    const unsigned char type = static_cast<unsigned char>(entry.type);
    const auto path_size = big_endian_uint64(entry.relative_path.size());
    const auto file_size = big_endian_uint64(entry.size);
    ok = ok && digest_update(context, &type, sizeof(type)) &&
         digest_update(context, path_size.data(), path_size.size()) &&
         digest_update(context, entry.relative_path.data(),
                       entry.relative_path.size()) &&
         digest_update(context, file_size.data(), file_size.size()) &&
         digest_update(context, entry.sha256.data(), entry.sha256.size());
  }
  std::array<unsigned char, EVP_MAX_MD_SIZE> output{};
  unsigned int output_size = 0;
  if (ok)
    ok = EVP_DigestFinal_ex(context, output.data(), &output_size) == 1 &&
         output_size == 32;
  EVP_MD_CTX_free(context);
  if (!ok) return fail(error, "cannot hash stopped-root manifest");
  *digest = hex_digest(output.data(), output_size);
  return true;
}

std::vector<char *> mutable_argv(const std::vector<std::string> &arguments) {
  std::vector<char *> result;
  result.reserve(arguments.size() + 1);
  for (const std::string &argument : arguments)
    result.push_back(const_cast<char *>(argument.c_str()));
  result.push_back(nullptr);
  return result;
}

}  // namespace

const char *startup_server_mode_name(StartupServerMode mode) {
  switch (mode) {
    case StartupServerMode::NORMAL:
      return "normal";
    case StartupServerMode::BOOTSTRAP_PREFLIGHT:
      return "bootstrap-preflight";
    case StartupServerMode::BOOTSTRAP_SNAPSHOT:
      return "bootstrap-snapshot";
    case StartupServerMode::TAKEOVER_RECOVERY:
      return "takeover-recovery";
    case StartupServerMode::INSTALLED_REEXEC:
      return "installed-reexec";
  }
  return "invalid";
}

bool parse_startup_server_mode(std::string_view name,
                               StartupServerMode *mode) {
  if (mode == nullptr) return false;
  static constexpr std::array<StartupServerMode, 5> kModes = {
      StartupServerMode::NORMAL, StartupServerMode::BOOTSTRAP_PREFLIGHT,
      StartupServerMode::BOOTSTRAP_SNAPSHOT,
      StartupServerMode::TAKEOVER_RECOVERY,
      StartupServerMode::INSTALLED_REEXEC};
  for (const StartupServerMode candidate : kModes) {
    if (name == startup_server_mode_name(candidate)) {
      *mode = candidate;
      return true;
    }
  }
  return false;
}

bool detect_declarative_remote_commit(int argc, char *const argv[],
                                      bool *enabled, std::string *error) {
  if (error != nullptr) error->clear();
  if (enabled == nullptr)
    return fail(error, "null declarative remote-commit output");
  *enabled = false;
  if (argc <= 0 || argv == nullptr)
    return fail(error, "defaults-expanded startup option vector is missing");
  for (int index = 0; index < argc; ++index) {
    if (argv[index] == nullptr)
      return fail(error,
                  "defaults-expanded startup option vector contains null");
    const ParsedRemoteCommitOption parsed =
        parse_remote_commit_option(argv[index]);
    if (parsed.matched && parsed.has_value) *enabled = parsed.value;
  }
  return true;
}

bool validate_remote_commit_option_source(bool declarative_enabled,
                                          bool effective_enabled,
                                          std::string *error) {
  if (error != nullptr) error->clear();
  if (effective_enabled && !declarative_enabled)
    return fail(error,
                "remote commit may not be enabled by mysqld-auto.cnf");
  return true;
}

bool validate_startup_external_security(
    const StartupExternalSecurityOptions &options, std::string *error) {
  if (error != nullptr) error->clear();
  if (options.auto_generate_certs)
    return fail(error,
                "remote commit requires --skip-auto-generate-certs");
  if (options.sha256_password_auto_generate_rsa_keys)
    return fail(
        error,
        "remote commit requires "
        "--skip-sha256-password-auto-generate-rsa-keys");
  if (options.caching_sha2_password_auto_generate_rsa_keys)
    return fail(
        error,
        "remote commit requires "
        "--skip-caching-sha2-password-auto-generate-rsa-keys");
  return true;
}

bool canonical_startup_data_directory(const fs::path &configured,
                                       fs::path *root, std::string *error) {
  if (error != nullptr) error->clear();
  if (root == nullptr) return fail(error, "null startup root output");
  if (configured.empty())
    return fail(error, "remote startup data directory is empty");

  std::error_code path_error;
  fs::path absolute = fs::absolute(configured, path_error).lexically_normal();
  if (path_error)
    return fail(error, "cannot resolve remote startup data directory");

  // libstdc++ preserves a trailing separator through lexical normalization,
  // which leaves filename() empty for an otherwise valid directory. Strip
  // that lexical spelling while keeping root-only paths rejected below.
  if (absolute.filename().empty() && absolute.parent_path() != absolute)
    absolute = absolute.parent_path();
  if (absolute.filename().empty())
    return fail(error, "cannot resolve remote startup data directory");

  const fs::path parent =
      fs::weakly_canonical(absolute.parent_path(), path_error);
  if (path_error || !fs::is_directory(parent, path_error) || path_error)
    return fail(error,
                "remote startup data-directory parent is not a real "
                "directory");
  *root = parent / absolute.filename();
  return true;
}

bool validate_startup_managed_path_options(int argc, char *const argv[],
                                           std::string *error) {
  if (error != nullptr) error->clear();
  if (argc <= 0 || argv == nullptr)
    return fail(error, "startup option vector is missing");
  std::array<bool, kManagedPathOptions.size()> seen{};
  for (int index = 0; index < argc; ++index) {
    if (argv[index] == nullptr)
      return fail(error, "startup option vector contains a null argument");
    const std::string_view argument(argv[index]);
    if (!argument.starts_with("--")) continue;
    const size_t delimiter = argument.find('=');
    const std::string_view raw_name = argument.substr(
        2, delimiter == std::string_view::npos
               ? std::string_view::npos
               : delimiter - 2);
    const ParsedManagedOptionName parsed =
        parse_managed_option_name(raw_name);
    if (parsed.ambiguous)
      return fail(error, "managed storage option has ambiguous prefixes");
    if (parsed.option == nullptr) continue;
    if (parsed.negated)
      return fail(error, "managed storage path option may not be negated");
    const size_t option_index =
        static_cast<size_t>(parsed.option - kManagedPathOptions.data());
    if (seen[option_index])
      return fail(error, "managed storage path option is duplicated");
    seen[option_index] = true;
    if (parsed.option->rule == ManagedPathRule::ABSENT) {
      std::string detail("remote commit forbids --");
      detail.append(parsed.option->name);
      return fail(error, std::move(detail));
    }

    std::string_view value;
    if (delimiter != std::string_view::npos) {
      value = argument.substr(delimiter + 1);
    } else {
      if (index + 1 >= argc || argv[index + 1] == nullptr ||
          std::string_view(argv[index + 1]).starts_with("--") ||
          is_startup_option_separator(argv[index + 1])) {
        return fail(error, "managed storage path option is missing its value");
      }
      value = argv[++index];
    }
    if (!managed_value_matches(*parsed.option, value)) {
      std::string detail("remote commit requires managed value for --");
      detail.append(parsed.option->name);
      return fail(error, std::move(detail));
    }
  }
  return true;
}

bool validate_innodb_startup_managed_paths(
    const StartupInnoDBManagedPaths &paths, std::string *error) {
  if (error != nullptr) error->clear();
  const std::array<std::pair<ManagedPathOption, std::string_view>, 8> values = {{
      {kManagedPathOptions[0], paths.data_home_dir},
      {kManagedPathOptions[1], paths.data_file_path},
      {kManagedPathOptions[2], paths.temp_data_file_path},
      {kManagedPathOptions[3], paths.log_group_home_dir},
      {kManagedPathOptions[4], paths.undo_directory},
      {kManagedPathOptions[5], paths.temp_tablespaces_dir},
      {kManagedPathOptions[6], paths.directories},
      {kManagedPathOptions[7], paths.doublewrite_dir},
  }};
  for (const auto &[option, value] : values) {
    if (!parsed_managed_value_matches(option, value)) {
      std::string detail("InnoDB path is outside the managed remote root: --");
      detail.append(option.name);
      return fail(error, std::move(detail));
    }
  }
  return true;
}

bool validate_smartengine_startup_managed_paths(
    const StartupSmartengineManagedPaths &paths, std::string *error) {
  if (error != nullptr) error->clear();
  const std::array<std::pair<ManagedPathOption, std::string_view>, 3> values = {{
      {kManagedPathOptions[8], paths.data_dir},
      {kManagedPathOptions[9], paths.wal_dir},
      {kManagedPathOptions[10], paths.persistent_cache_dir},
  }};
  for (const auto &[option, value] : values) {
    if (!managed_value_matches(option, value)) {
      std::string detail(
          "SmartEngine path is outside the managed remote root: --");
      detail.append(option.name);
      return fail(error, std::move(detail));
    }
  }
  if (paths.persistent_cache_size != 0)
    return fail(error,
                "SmartEngine persistent cache must be disabled in remote "
                "commit mode");
  return true;
}

bool create_startup_control_directory(const fs::path &target_root,
                                      fs::path *control_directory,
                                      std::string *error) {
  if (control_directory == nullptr)
    return fail(error, "null startup control-directory output");
  std::error_code path_error;
  fs::path absolute_target = fs::absolute(target_root, path_error);
  if (path_error || absolute_target.filename().empty())
    return fail(error, "cannot resolve target root for startup control");
  const fs::path parent = absolute_target.parent_path();
  const fs::file_status parent_status = fs::symlink_status(parent, path_error);
  if (path_error || !fs::is_directory(parent_status) ||
      fs::is_symlink(parent_status))
    return fail(error, "target parent is not a real directory");

  std::string pattern =
      (parent / ".wesql-remote-startup-XXXXXX").string();
  std::vector<char> writable(pattern.begin(), pattern.end());
  writable.push_back('\0');
  char *created = ::mkdtemp(writable.data());
  if (created == nullptr)
    return fail(error, errno_detail("cannot create startup control directory"));
  if (::chmod(created, S_IRWXU) != 0)
    return fail(error, errno_detail("cannot protect startup control directory"));
  *control_directory = fs::path(created);
  return fsync_directory(parent, error);
}

bool remove_startup_control_directory(const fs::path &control_directory,
                                      std::string *error) {
  constexpr std::string_view kPrefix = ".wesql-remote-startup-";
  const std::string name = control_directory.filename().string();
  if (control_directory.empty() || !control_directory.is_absolute() ||
      control_directory.lexically_normal() != control_directory ||
      name.size() != kPrefix.size() + 6 || !name.starts_with(kPrefix) ||
      !std::all_of(name.begin() + static_cast<std::ptrdiff_t>(kPrefix.size()),
                   name.end(), [](unsigned char byte) {
                     return std::isalnum(byte) != 0;
                   })) {
    return fail(error, "startup control directory identity is invalid");
  }
  const fs::path parent = control_directory.parent_path();
  std::error_code status_error;
  const fs::file_status status =
      fs::symlink_status(control_directory, status_error);
  if (status_error || !fs::is_directory(status) || fs::is_symlink(status))
    return fail(error, "startup control path is not a real directory");
  const uintmax_t removed = fs::remove_all(control_directory, status_error);
  if (status_error || removed == 0 || fs::exists(control_directory, status_error) ||
      status_error)
    return fail(error, "cannot remove verified startup control directory");
  return fsync_directory(parent, error);
}

bool write_startup_proof(const fs::path &path, std::string_view payload,
                         StartupProofReference *reference,
                         std::string *error, uint64_t maximum_bytes) {
  return publish_startup_output_proof(path, payload, reference, error,
                                      maximum_bytes);
}

bool publish_startup_output_proof(const fs::path &path,
                                  std::string_view payload,
                                  StartupProofReference *reference,
                                  std::string *error,
                                  uint64_t maximum_bytes) {
  if (reference == nullptr) return fail(error, "null startup proof reference");
  *reference = {};
  if (!absolute_path(path, "startup proof", error)) return false;
  const fs::path exact_path = path.lexically_normal();
  if (exact_path != path)
    return fail(error, "startup proof path is not lexically canonical");
  if (payload.empty()) return fail(error, "startup proof payload is empty");
  if (maximum_bytes == 0)
    return fail(error, "startup proof payload bound is zero");
  if (payload.size() > maximum_bytes)
    return fail(error, "startup proof payload exceeds its bound");
  std::error_code status_error;
  const fs::file_status parent_status =
      fs::symlink_status(path.parent_path(), status_error);
  if (status_error || !fs::is_directory(parent_status) ||
      fs::is_symlink(parent_status))
    return fail(error, "startup proof parent is not a real directory");
  if (fs::exists(path, status_error) || status_error)
    return fail(error, "startup proof destination already exists");

  std::string temporary_pattern = path.string() + ".tmp-XXXXXX";
  std::vector<char> temporary_name(temporary_pattern.begin(),
                                   temporary_pattern.end());
  temporary_name.push_back('\0');
  const int descriptor = ::mkstemp(temporary_name.data());
  if (descriptor < 0)
    return fail(error, errno_detail("cannot create startup proof file"));
  const fs::path temporary(temporary_name.data());
  bool ok = ::fchmod(descriptor, S_IRUSR | S_IWUSR) == 0 &&
            ::fcntl(descriptor, F_SETFD, FD_CLOEXEC) == 0;
  if (!ok) fail(error, errno_detail("cannot protect startup proof file"));
  if (ok) ok = write_all(descriptor, payload, error);
  if (ok && ::fsync(descriptor) != 0)
    ok = fail(error, errno_detail("cannot fsync startup proof file"));
  if (::close(descriptor) != 0 && ok)
    ok = fail(error, errno_detail("cannot close startup proof file"));
  if (!ok) {
    (void)::unlink(temporary.c_str());
    return false;
  }
  if (::rename(temporary.c_str(), path.c_str()) != 0) {
    const std::string detail = errno_detail("cannot publish startup proof file");
    (void)::unlink(temporary.c_str());
    return fail(error, detail);
  }
  if (!fsync_directory(path.parent_path(), error)) return false;

  std::string digest;
  if (!sha256_bytes(payload, &digest, error)) return false;
  StartupProofReference published{exact_path,
                                  static_cast<uint64_t>(payload.size()),
                                  std::move(digest)};
  std::string verified;
  if (!read_startup_proof(published, &verified, error, maximum_bytes) ||
      verified != payload) {
    return false;
  }
  *reference = std::move(published);
  return true;
}

bool read_startup_proof(const StartupProofReference &reference,
                        std::string *payload, std::string *error,
                        uint64_t maximum_bytes) {
  if (payload == nullptr) return fail(error, "null startup proof payload");
  payload->clear();
  if (!valid_reference(reference, maximum_bytes, error)) return false;
  const int descriptor =
      ::open(reference.path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (descriptor < 0)
    return fail(error, errno_detail("cannot open startup proof file"));
  struct stat status {};
  if (::fstat(descriptor, &status) != 0 || !S_ISREG(status.st_mode) ||
      status.st_size < 0 ||
      static_cast<uint64_t>(status.st_size) != reference.size) {
    ::close(descriptor);
    return fail(error, "startup proof file shape or size changed");
  }
  payload->resize(static_cast<size_t>(reference.size));
  size_t consumed = 0;
  while (consumed < payload->size()) {
    const ssize_t count = ::read(descriptor, payload->data() + consumed,
                                 payload->size() - consumed);
    if (count < 0 && errno == EINTR) continue;
    if (count <= 0) {
      ::close(descriptor);
      payload->clear();
      return fail(error, "startup proof file is truncated");
    }
    consumed += static_cast<size_t>(count);
  }
  char trailing = 0;
  const ssize_t trailing_count = ::read(descriptor, &trailing, 1);
  if (::close(descriptor) != 0 || trailing_count != 0) {
    payload->clear();
    return fail(error, "startup proof file has trailing bytes or close failed");
  }
  std::string digest;
  if (!sha256_bytes(*payload, &digest, error) || digest != reference.sha256) {
    payload->clear();
    return fail(error, "startup proof SHA-256 does not match its reference");
  }
  return true;
}

bool remove_startup_proof(const StartupProofReference &reference,
                          std::string *error) {
  if (!absolute_path(reference.path, "startup proof", error)) return false;
  if (::unlink(reference.path.c_str()) != 0)
    return fail(error, errno_detail("cannot remove startup proof file"));
  return fsync_directory(reference.path.parent_path(), error);
}

bool build_startup_child_argv(const std::vector<std::string> &original,
                              const StartupChildSpec &spec,
                              std::vector<std::string> *child,
                              std::string *error) {
  if (child == nullptr) return fail(error, "null child argv output");
  child->clear();
  if (original.empty() || original.front().empty() ||
      spec.mode == StartupServerMode::NORMAL ||
      !absolute_path(spec.data_directory, "child data directory", error))
    return false;
  const bool installed = spec.mode == StartupServerMode::INSTALLED_REEXEC;
  if ((!installed &&
       (!absolute_path(spec.pid_file, "child pid file", error) ||
        !absolute_path(spec.error_log, "child error log", error))) ||
      (installed && (!spec.pid_file.empty() || !spec.error_log.empty())))
    return false;
  if ((!installed && !absolute_path(spec.output_path, "child output", error)) ||
      (installed && !spec.output_path.empty()))
    return false;
  if (spec.mode != StartupServerMode::BOOTSTRAP_PREFLIGHT &&
      !valid_reference(spec.input, kStartupProofMaxBytes, error))
    return false;
  if (spec.mode == StartupServerMode::BOOTSTRAP_PREFLIGHT &&
      (!spec.input.path.empty() || spec.input.size != 0 ||
       !spec.input.sha256.empty()))
    return fail(error, "bootstrap preflight must not carry an input proof");
  if (spec.initialize_insecure !=
      (spec.mode == StartupServerMode::BOOTSTRAP_PREFLIGHT))
    return fail(error, "initialize-insecure is valid only for preflight");
  if ((installed && spec.inherited_daemon_pipe_fd != -1 &&
       spec.inherited_daemon_pipe_fd <= STDERR_FILENO) ||
      (!installed && spec.inherited_daemon_pipe_fd != -1) ||
      spec.inherited_daemon_pipe_fd < -1)
    return fail(error, "startup child has an invalid inherited daemon pipe");

  child->push_back(original.front());
  for (size_t index = 1; index < original.size(); ++index) {
    if (!validate_defaults_path_option(original, index, error)) return false;
    if (is_print_defaults_option(original[index]))
      return fail(error,
                  "startup child orchestration rejects --print-defaults");
    bool takes_next = false;
    if (remove_option(original[index], !installed, &takes_next)) {
      if (takes_next) {
        if (index + 1 >= original.size())
          return fail(error, "startup option is missing its value");
        ++index;
      }
      continue;
    }
    child->push_back(original[index]);
  }

  child->push_back(std::string(kModeOption) + "=" +
                   startup_server_mode_name(spec.mode));
  if (!append_option("--datadir", spec.data_directory, child, error))
    return false;
  if (!installed) {
    if (!append_option("--pid-file", spec.pid_file, child, error) ||
        !append_option("--log-error", spec.error_log, child, error))
      return false;
    child->push_back("--skip-networking");
  }
  // The X Plugin cannot defer its listener until installed-root evidence has
  // opened admission. P0 therefore keeps it disabled in every internal mode.
  child->push_back("--skip-mysqlx");
  child->push_back("--skip-daemonize");
  child->push_back("--skip-initialize");
  child->push_back("--skip-initialize-insecure");
  if (spec.mode != StartupServerMode::BOOTSTRAP_PREFLIGHT) {
    if (!append_option(kInputOption, spec.input.path, child, error)) return false;
    child->push_back(std::string(kInputSizeOption) + "=" +
                     std::to_string(spec.input.size));
    child->push_back(std::string(kInputShaOption) + "=" + spec.input.sha256);
  }
  if (!installed &&
      !append_option(kOutputOption, spec.output_path, child, error))
    return false;
  if (spec.inherited_daemon_pipe_fd >= 0) {
    child->push_back(std::string(kDaemonPipeOption) + "=" +
                     std::to_string(spec.inherited_daemon_pipe_fd));
  }
  if (spec.initialize_insecure) {
    // SmartEngine first opens in the snapshot worker, after epoch adoption.
    child->push_back("--skip-smartengine");
    child->push_back("--default-storage-engine=InnoDB");
    child->push_back("--initialize-insecure");
  }
  return true;
}

bool resolve_current_executable(std::string_view argv0, fs::path *executable,
                                std::string *error) {
  if (executable == nullptr) return fail(error, "null executable output");
#if defined(__linux__)
  std::array<char, 4096> path{};
  const ssize_t length =
      ::readlink("/proc/self/exe", path.data(), path.size() - 1);
  if (length > 0 && static_cast<size_t>(length) < path.size()) {
    *executable = fs::path(std::string(path.data(), static_cast<size_t>(length)));
    return true;
  }
#elif defined(__APPLE__)
  uint32_t length = 0;
  (void)_NSGetExecutablePath(nullptr, &length);
  std::vector<char> path(length + 1, '\0');
  if (length > 0 && _NSGetExecutablePath(path.data(), &length) == 0) {
    std::error_code canonical_error;
    *executable = fs::weakly_canonical(path.data(), canonical_error);
    if (!canonical_error) return true;
  }
#endif
  if (argv0.empty()) return fail(error, "cannot resolve current executable");
  std::error_code path_error;
  *executable = fs::absolute(fs::path(argv0), path_error);
  if (path_error || executable->filename().empty())
    return fail(error, "cannot resolve argv[0] as an executable path");
  return true;
}

StartupProcessResult spawn_startup_child(
    const fs::path &executable, const std::vector<std::string> &arguments) {
  StartupProcessResult result;
  if (executable.empty() || arguments.empty()) {
    result.detail = "startup child executable or argv is empty";
    return result;
  }
  std::vector<char *> argv = mutable_argv(arguments);
  posix_spawnattr_t attributes;
  if (posix_spawnattr_init(&attributes) != 0) {
    result.detail = "cannot initialize startup child attributes";
    return result;
  }
  short flags = POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETSIGDEF |
                POSIX_SPAWN_SETPGROUP;
  sigset_t empty_mask;
  sigset_t default_signals;
  sigemptyset(&empty_mask);
  sigemptyset(&default_signals);
  sigaddset(&default_signals, SIGINT);
  sigaddset(&default_signals, SIGTERM);
  sigaddset(&default_signals, SIGHUP);
  bool attributes_ok = posix_spawnattr_setflags(&attributes, flags) == 0 &&
                       posix_spawnattr_setsigmask(&attributes, &empty_mask) ==
                           0 &&
                       posix_spawnattr_setsigdefault(&attributes,
                                                     &default_signals) == 0 &&
                       posix_spawnattr_setpgroup(&attributes, 0) == 0;
  pid_t child = -1;
  const int spawn_error =
      attributes_ok
          ? posix_spawn(&child, executable.c_str(), nullptr, &attributes,
                        argv.data(), environ)
          : EINVAL;
  posix_spawnattr_destroy(&attributes);
  if (spawn_error != 0) {
    result.detail = "cannot spawn startup child: ";
    result.detail.append(std::strerror(spawn_error));
    return result;
  }
  result.started = true;
  int status = 0;
  pid_t waited = -1;
  do {
    waited = ::waitpid(child, &status, 0);
  } while (waited < 0 && errno == EINTR);
  if (waited != child) {
    result.detail = errno_detail("cannot wait for startup child");
    return result;
  }
  if (WIFEXITED(status)) {
    result.exited = true;
    result.exit_code = WEXITSTATUS(status);
    if (result.exit_code != 0)
      result.detail = "startup child exited with status " +
                      std::to_string(result.exit_code);
  } else if (WIFSIGNALED(status)) {
    result.signal_number = WTERMSIG(status);
    result.detail = "startup child terminated by signal " +
                    std::to_string(result.signal_number);
  } else {
    result.detail = "startup child ended in an unsupported wait state";
  }
  return result;
}

bool reexec_startup_server(const fs::path &executable,
                           const std::vector<std::string> &arguments,
                           std::string *error) {
  if (executable.empty() || arguments.empty())
    return fail(error, "startup re-exec executable or argv is empty");
  std::vector<char *> argv = mutable_argv(arguments);
  ::execv(executable.c_str(), argv.data());
  return fail(error, errno_detail("cannot re-exec installed startup root"));
}

bool snapshot_stable_startup_root(const fs::path &root,
                                  StartupRootSnapshot *snapshot,
                                  std::string *error, size_t maximum_entries,
                                  uint64_t maximum_regular_file_bytes) {
  if (snapshot == nullptr) return fail(error, "null stopped-root snapshot");
  *snapshot = {};
  if (maximum_entries == 0)
    return fail(error, "stopped-root entry bound is zero");
  std::error_code root_error;
  const fs::file_status root_status = fs::symlink_status(root, root_error);
  if (root_error || !fs::is_directory(root_status) ||
      fs::is_symlink(root_status))
    return fail(error, "stopped root is not a real directory");

  fs::recursive_directory_iterator iterator(root, root_error);
  const fs::recursive_directory_iterator end;
  while (!root_error && iterator != end) {
    if (snapshot->entries.size() >= maximum_entries)
      return fail(error, "stopped root exceeds its entry bound");
    const fs::directory_entry &entry = *iterator;
    const fs::file_status status = entry.symlink_status(root_error);
    if (root_error) break;
    StartupRootEntry observed;
    const fs::path relative = entry.path().lexically_relative(root);
    if (!valid_relative_path(relative, &observed.relative_path))
      return fail(error, "stopped root contains an invalid relative path");
    if (fs::is_directory(status)) {
      observed.type = StartupRootEntryType::DIRECTORY;
    } else if (fs::is_regular_file(status)) {
      observed.type = StartupRootEntryType::REGULAR_FILE;
      if (!hash_regular_file(entry.path(), &observed.size, &observed.sha256,
                             error))
        return false;
      if (observed.size > maximum_regular_file_bytes -
                              snapshot->manifest.regular_file_bytes)
        return fail(error, "stopped root exceeds its byte bound");
      snapshot->manifest.regular_file_bytes += observed.size;
      ++snapshot->manifest.regular_file_count;
    } else {
      return fail(error,
                  "stopped root contains a symlink or special filesystem entry");
    }
    snapshot->entries.push_back(std::move(observed));
    iterator.increment(root_error);
  }
  if (root_error) return fail(error, "cannot enumerate stopped root");
  std::sort(snapshot->entries.begin(), snapshot->entries.end(),
            [](const StartupRootEntry &left, const StartupRootEntry &right) {
              return left.relative_path < right.relative_path;
            });
  snapshot->manifest.entry_count = snapshot->entries.size();
  return hash_root_manifest(*snapshot, &snapshot->manifest.manifest_sha256,
                            error);
}

}  // namespace wesql::remote_commit
