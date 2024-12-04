#pragma once
#include <buffer/lib_aio.hpp>
#include <common/type.hpp>

class VMBufferManager {
public:
  static const u64 mb = 1024ull * 1024;
  static const u64 gb = 1024ull * 1024 * 1024;

  // FIXME: wrap these in a struct
  // used for static analysis
  atomic<u64> allocCount;
  atomic<u64> readCount;
  atomic<u64> writeCount;

  VMBufferManager();
  ~VMBufferManager() {}

  Page *fixX(PID pid);
  void unfixX(PID pid);
  Page *fixS(PID pid);
  void unfixS(PID pid);

  Page *toPtr(PID pid) { return virtMem + pid; }
  PID toPID(void *page) { return reinterpret_cast<Page *>(page) - virtMem; }
  PageState &getPageState(PID pid) { return pageState[pid]; }
  Page *allocPage();
  void handleFault(PID pid);

  u64 getBatch() const { return batch; }

  // only for vmcache
  bool isValidPtr(void *page) {
    return (page >= virtMem) && (page < (virtMem + virtSize + 16));
  }

private:
  u64 virtSize;
  u64 physSize;
  u64 virtCount;
  u64 physCount;
  vector<LibaioInterface> libaioInterface;

  int blockfd;

  atomic<u64> physUsedCount;
  ResidentPageSet residentSet;

  Page *virtMem;
  PageState *pageState;
  // used for clock eviction
  u64 batch;

  void ensureFreePages();
  void readPage(PID pid);
  void evict();
};