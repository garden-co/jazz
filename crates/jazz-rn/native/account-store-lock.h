#ifndef JAZZ_RN_ACCOUNT_STORE_LOCK_H
#define JAZZ_RN_ACCOUNT_STORE_LOCK_H

#include <cerrno>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <sys/file.h>
#include <unistd.h>

namespace jazz::rn {

/** Cross-runtime/process lock for short synchronous OS secure-store updates.
 * The platform supplies the directory; JavaScript cannot choose a lock path.
 * The OS releases the lock on process death. Never hold a runtime lifecycle
 * mutex while acquiring this lock or invoking the protected callback.
 */
class AccountStoreLock final {
 public:
  explicit AccountStoreLock(const std::string &storageRoot) {
    if (heldOnThread_) throw std::runtime_error("Jazz account store update is not reentrant");
    if (storageRoot.empty()) throw std::runtime_error("Jazz account store has no native storage root");
    const auto path = storageRoot + "/account-selection-v1.lock";
    do {
      fd_ = ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
    } while (fd_ < 0 && errno == EINTR);
    if (fd_ < 0) throw std::runtime_error("Jazz account store lock could not be opened");
    int status;
    do {
      status = ::flock(fd_, LOCK_EX);
    } while (status < 0 && errno == EINTR);
    if (status < 0) {
      ::close(fd_);
      fd_ = -1;
      throw std::runtime_error("Jazz account store lock could not be acquired");
    }
    heldOnThread_ = true;
  }
  ~AccountStoreLock() {
    if (fd_ >= 0) {
      ::flock(fd_, LOCK_UN);
      ::close(fd_);
      heldOnThread_ = false;
    }
  }
  AccountStoreLock(const AccountStoreLock &) = delete;
  AccountStoreLock &operator=(const AccountStoreLock &) = delete;

 private:
  int fd_{-1};
  inline static thread_local bool heldOnThread_{false};
};

}  // namespace jazz::rn
#endif
