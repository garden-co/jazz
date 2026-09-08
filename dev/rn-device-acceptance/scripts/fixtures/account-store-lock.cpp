#include "account-store-lock.h"
#include <cassert>
#include <fstream>
#include <signal.h>
#include <sys/wait.h>
#include <vector>

int main(int argc, char **argv) {
  assert(argc == 2);
  const std::string root = argv[1];
  const auto valuePath = root + "/value";
  { std::ofstream out(valuePath); out << 0; }
  std::vector<pid_t> writers;
  for (int writer = 0; writer < 4; ++writer) {
    const auto pid = fork();
    assert(pid >= 0);
    if (pid == 0) {
      for (int update = 0; update < 40; ++update) {
        jazz::rn::AccountStoreLock lock(root);
        int value = -1;
        { std::ifstream in(valuePath); in >> value; }
        assert(value >= 0);
        usleep(100);
        { std::ofstream out(valuePath); out << value + 1; }
      }
      _exit(0);
    }
    writers.push_back(pid);
  }
  for (const auto pid : writers) {
    int status;
    assert(waitpid(pid, &status, 0) == pid);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  }
  int total = -1;
  { std::ifstream in(valuePath); in >> total; }
  assert(total == 160);

  {
    jazz::rn::AccountStoreLock lock(root);
    bool rejected = false;
    try { jazz::rn::AccountStoreLock nested(root); }
    catch (const std::runtime_error &) { rejected = true; }
    assert(rejected);
  }
  try {
    jazz::rn::AccountStoreLock lock(root);
    throw std::runtime_error("simulated secure-store failure");
  } catch (const std::runtime_error &) {}
  { jazz::rn::AccountStoreLock afterFailure(root); }

  int ready[2];
  assert(pipe(ready) == 0);
  const auto holder = fork();
  assert(holder >= 0);
  if (holder == 0) {
    close(ready[0]);
    jazz::rn::AccountStoreLock lock(root);
    assert(write(ready[1], "x", 1) == 1);
    for (;;) pause();
  }
  close(ready[1]);
  char signal;
  assert(read(ready[0], &signal, 1) == 1);
  close(ready[0]);
  assert(kill(holder, SIGKILL) == 0);
  int status;
  assert(waitpid(holder, &status, 0) == holder);
  { jazz::rn::AccountStoreLock afterCrash(root); }
  return 0;
}
