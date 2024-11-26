#pragma once
#include <iostream>
#include <vector>

#include <libaio.h>

#include <common/pages.hpp>
#include <tpcc/Types.hpp>

using std::vector;

// libaio interface used to write batches of pages
struct LibaioInterface {
  static const u64 maxIOs = 256;

  int blockfd;
  Page *virtMem;
  io_context_t ctx;
  iocb cb[maxIOs];
  iocb *cbPtr[maxIOs];
  io_event events[maxIOs];

  LibaioInterface(int blockfd, Page *virtMem)
      : blockfd(blockfd), virtMem(virtMem) {
    memset(&ctx, 0, sizeof(io_context_t));
    int ret = io_setup(maxIOs, &ctx);
    if (ret != 0) {
      std::cerr << "libaio io_setup error: " << ret << " ";
      switch (-ret) {
      case EAGAIN:
        std::cerr << "EAGAIN";
        break;
      case EFAULT:
        std::cerr << "EFAULT";
        break;
      case EINVAL:
        std::cerr << "EINVAL";
        break;
      case ENOMEM:
        std::cerr << "ENOMEM";
        break;
      case ENOSYS:
        std::cerr << "ENOSYS";
        break;
      };
      exit(EXIT_FAILURE);
    }
  }

  void writePages(const vector<PID> &pages) {
    assert(pages.size() < maxIOs);
    for (u64 i = 0; i < pages.size(); i++) {
      PID pid = pages[i];
      virtMem[pid].dirty = false;
      cbPtr[i] = &cb[i];
      io_prep_pwrite(cb + i, blockfd, &virtMem[pid], pageSize, pageSize * pid);
    }
    int cnt = io_submit(ctx, pages.size(), cbPtr);
    assert(cnt == pages.size());
    cnt = io_getevents(ctx, pages.size(), pages.size(), events, nullptr);
    assert(cnt == pages.size());
  }
};