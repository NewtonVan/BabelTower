#pragma once
#include <buffer/buffer_manager.hpp>
#include <buffer/lib_aio.hpp>
#include <common/type.hpp>

class VMBufferManager : public IBufferManager {
public:
  // FIXME: wrap these in a struct
  // used for static analysis
  atomic<u64> allocCount;
  atomic<u64> readCount;
  atomic<u64> writeCount;

  VMBufferManager();
  ~VMBufferManager() {}

  Page *fixX(PID pid) override;
  void unfixX(PID pid) override;
  Page *fixS(PID pid) override;
  void unfixS(PID pid) override;

  Page *toPtr(PID pid) override { return virtMem + pid; }
  PID toPID(void *page) override {
    return reinterpret_cast<Page *>(page) - virtMem;
  }
  PageState &getPageState(PID pid) override { return pageState[pid]; }
  Page *allocPage() override;
  void handleFault(PID pid) override;

  // only for vmcache
  u64 getBatch() const { return batch; }

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