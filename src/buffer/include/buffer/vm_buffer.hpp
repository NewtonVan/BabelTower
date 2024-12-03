#pragma once
#include <buffer/lib_aio.hpp>
#include <common/type.hpp>

struct VMBufferManager {
  static const u64 mb = 1024ull * 1024;
  static const u64 gb = 1024ull * 1024 * 1024;
  u64 virtSize;
  u64 physSize;
  u64 virtCount;
  u64 physCount;
  vector<LibaioInterface> libaioInterface;

  int blockfd;

  atomic<u64> physUsedCount;
  ResidentPageSet residentSet;
  atomic<u64> allocCount;

  atomic<u64> readCount;
  atomic<u64> writeCount;

  Page *virtMem;
  PageState *pageState;
  u64 batch;

  PageState &getPageState(PID pid) { return pageState[pid]; }

  VMBufferManager();
  ~VMBufferManager() {}

  Page *fixX(PID pid);
  void unfixX(PID pid);
  Page *fixS(PID pid);
  void unfixS(PID pid);

  bool isValidPtr(void *page) {
    return (page >= virtMem) && (page < (virtMem + virtSize + 16));
  }
  PID toPID(void *page) { return reinterpret_cast<Page *>(page) - virtMem; }
  Page *toPtr(PID pid) { return virtMem + pid; }

  void ensureFreePages();
  Page *allocPage();
  void handleFault(PID pid);
  void readPage(PID pid);
  void evict();
};