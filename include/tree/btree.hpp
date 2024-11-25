#pragma once
#include <algorithm>
#include <atomic>
#include <cassert>
#include <csignal>
#include <exception>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <mutex>
#include <numeric>
#include <set>
#include <span>
#include <thread>
#include <vector>

#include <errno.h>
#include <immintrin.h>
#include <libaio.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "tpcc/TPCCWorkload.hpp"

using namespace std;

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef u64 PID; // page id type

static const u64 pageSize = 4096;

struct alignas(4096) Page {
  bool dirty;
};

static const int16_t maxWorkerThreads = 128;

#define die(msg)                                                               \
  do {                                                                         \
    perror(msg);                                                               \
    exit(EXIT_FAILURE);                                                        \
  } while (0)

inline uint64_t rdtsc() {
  uint32_t hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return static_cast<uint64_t>(lo) | (static_cast<uint64_t>(hi) << 32);
}

// allocate memory using huge pages
inline void *allocHuge(size_t size) {
  void *p = mmap(NULL, size, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  madvise(p, size, MADV_HUGEPAGE);
  return p;
}

// use when lock is not free
inline void yield(u64 counter) { _mm_pause(); }

struct PageState {
  atomic<u64> stateAndVersion;

  static const u64 Unlocked = 0;
  static const u64 MaxShared = 252;
  static const u64 Locked = 253;
  static const u64 Marked = 254;
  static const u64 Evicted = 255;

  PageState() {}

  void init() {
    stateAndVersion.store(sameVersion(0, Evicted), std::memory_order_release);
  }

  static inline u64 sameVersion(u64 oldStateAndVersion, u64 newState) {
    return ((oldStateAndVersion << 8) >> 8) | newState << 56;
  }
  static inline u64 nextVersion(u64 oldStateAndVersion, u64 newState) {
    return (((oldStateAndVersion << 8) >> 8) + 1) | newState << 56;
  }

  bool tryLockX(u64 oldStateAndVersion) {
    return stateAndVersion.compare_exchange_strong(
        oldStateAndVersion, sameVersion(oldStateAndVersion, Locked));
  }

  void unlockX() {
    assert(getState() == Locked);
    stateAndVersion.store(nextVersion(stateAndVersion.load(), Unlocked),
                          std::memory_order_release);
  }

  void unlockXEvicted() {
    assert(getState() == Locked);
    stateAndVersion.store(nextVersion(stateAndVersion.load(), Evicted),
                          std::memory_order_release);
  }

  void downgradeLock() {
    assert(getState() == Locked);
    stateAndVersion.store(nextVersion(stateAndVersion.load(), 1),
                          std::memory_order_release);
  }

  bool tryLockS(u64 oldStateAndVersion) {
    u64 s = getState(oldStateAndVersion);
    if (s < MaxShared)
      return stateAndVersion.compare_exchange_strong(
          oldStateAndVersion, sameVersion(oldStateAndVersion, s + 1));
    if (s == Marked)
      return stateAndVersion.compare_exchange_strong(
          oldStateAndVersion, sameVersion(oldStateAndVersion, 1));
    return false;
  }

  void unlockS() {
    while (true) {
      u64 oldStateAndVersion = stateAndVersion.load();
      u64 state = getState(oldStateAndVersion);
      assert(state > 0 && state <= MaxShared);
      if (stateAndVersion.compare_exchange_strong(
              oldStateAndVersion, sameVersion(oldStateAndVersion, state - 1)))
        return;
    }
  }

  bool tryMark(u64 oldStateAndVersion) {
    assert(getState(oldStateAndVersion) == Unlocked);
    return stateAndVersion.compare_exchange_strong(
        oldStateAndVersion, sameVersion(oldStateAndVersion, Marked));
  }

  static u64 getState(u64 v) { return v >> 56; };
  u64 getState() { return getState(stateAndVersion.load()); }

  void operator=(PageState &) = delete;
};

// open addressing hash table used for second chance replacement to keep track
// of currently-cached pages
struct ResidentPageSet {
  static const u64 empty = ~0ull;
  static const u64 tombstone = (~0ull) - 1;

  struct Entry {
    atomic<u64> pid;
  };

  Entry *ht;
  u64 count;
  u64 mask;
  atomic<u64> clockPos;

  ResidentPageSet(u64 maxCount)
      : count(next_pow2(maxCount * 1.5)), mask(count - 1), clockPos(0) {
    ht = (Entry *)allocHuge(count * sizeof(Entry));
    memset((void *)ht, 0xFF, count * sizeof(Entry));
  }

  ~ResidentPageSet() { munmap(ht, count * sizeof(u64)); }

  u64 next_pow2(u64 x) { return 1 << (64 - __builtin_clzl(x - 1)); }

  u64 hash(u64 k) {
    const u64 m = 0xc6a4a7935bd1e995;
    const int r = 47;
    u64 h = 0x8445d61a4e774912 ^ (8 * m);
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
  }

  void insert(u64 pid) {
    u64 pos = hash(pid) & mask;
    while (true) {
      u64 curr = ht[pos].pid.load();
      assert(curr != pid);
      if ((curr == empty) || (curr == tombstone))
        if (ht[pos].pid.compare_exchange_strong(curr, pid))
          return;

      pos = (pos + 1) & mask;
    }
  }

  bool remove(u64 pid) {
    u64 pos = hash(pid) & mask;
    while (true) {
      u64 curr = ht[pos].pid.load();
      if (curr == empty)
        return false;

      if (curr == pid)
        if (ht[pos].pid.compare_exchange_strong(curr, tombstone))
          return true;

      pos = (pos + 1) & mask;
    }
  }

  template <class Fn> void iterateClockBatch(u64 batch, Fn fn) {
    u64 pos, newPos;
    do {
      pos = clockPos.load();
      newPos = (pos + batch) % count;
    } while (!clockPos.compare_exchange_strong(pos, newPos));

    for (u64 i = 0; i < batch; i++) {
      u64 curr = ht[pos].pid.load();
      if ((curr != tombstone) && (curr != empty))
        fn(curr);
      pos = (pos + 1) & mask;
    }
  }
};

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

struct BufferManager {
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

  BufferManager();
  ~BufferManager() {}

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

struct ExecContext {
  // TODO(chen): use a uniq ptr?
  BufferManager bm_;
  static ExecContext &getGlobalContext();

private:
  static ExecContext global_ctx;
};

struct OLCRestartException {};

template <class T> struct GuardO {
  PID pid;
  T *ptr;
  u64 version;
  static const u64 moved = ~0ull;

  // constructor
  explicit GuardO(u64 pid)
      : pid(pid), ptr(reinterpret_cast<T *>(
                      ExecContext::getGlobalContext().bm_.toPtr(pid))) {
    init();
  }

  template <class T2> GuardO(u64 pid, GuardO<T2> &parent) {
    parent.checkVersionAndRestart();
    this->pid = pid;
    ptr = reinterpret_cast<T *>(ExecContext::getGlobalContext().bm_.toPtr(pid));
    init();
  }

  GuardO(GuardO &&other) {
    pid = other.pid;
    ptr = other.ptr;
    version = other.version;
  }

  void init() {
    assert(pid != moved);
    PageState &ps = ExecContext::getGlobalContext().bm_.getPageState(pid);
    for (u64 repeatCounter = 0;; repeatCounter++) {
      u64 v = ps.stateAndVersion.load();
      switch (PageState::getState(v)) {
      case PageState::Marked: {
        u64 newV = PageState::sameVersion(v, PageState::Unlocked);
        if (ps.stateAndVersion.compare_exchange_weak(v, newV)) {
          version = newV;
          return;
        }
        break;
      }
      case PageState::Locked:
        break;
      case PageState::Evicted:
        if (ps.tryLockX(v)) {
          ExecContext::getGlobalContext().bm_.handleFault(pid);
          ExecContext::getGlobalContext().bm_.unfixX(pid);
        }
        break;
      default:
        version = v;
        return;
      }
      yield(repeatCounter);
    }
  }

  // move assignment operator
  GuardO &operator=(GuardO &&other) {
    if (pid != moved)
      checkVersionAndRestart();
    pid = other.pid;
    ptr = other.ptr;
    version = other.version;
    other.pid = moved;
    other.ptr = nullptr;
    return *this;
  }

  // assignment operator
  GuardO &operator=(const GuardO &) = delete;

  // copy constructor
  GuardO(const GuardO &) = delete;

  void checkVersionAndRestart() {
    if (pid != moved) {
      PageState &ps = ExecContext::getGlobalContext().bm_.getPageState(pid);
      u64 stateAndVersion = ps.stateAndVersion.load();
      if (version == stateAndVersion) // fast path, nothing changed
        return;
      if ((stateAndVersion << 8) == (version << 8)) { // same version
        u64 state = PageState::getState(stateAndVersion);
        if (state <= PageState::MaxShared)
          return; // ignore shared locks
        if (state == PageState::Marked)
          if (ps.stateAndVersion.compare_exchange_weak(
                  stateAndVersion,
                  PageState::sameVersion(stateAndVersion, PageState::Unlocked)))
            return; // mark cleared
      }
      if (std::uncaught_exceptions() == 0)
        throw OLCRestartException();
    }
  }

  // destructor
  ~GuardO() noexcept(false) { checkVersionAndRestart(); }

  T *operator->() {
    assert(pid != moved);
    return ptr;
  }

  void release() {
    checkVersionAndRestart();
    pid = moved;
    ptr = nullptr;
  }
};

template <class T> struct GuardX {
  PID pid;
  T *ptr;
  static const u64 moved = ~0ull;

  // constructor
  GuardX() : pid(moved), ptr(nullptr) {}

  // constructor
  explicit GuardX(u64 pid) : pid(pid) {
    ptr = reinterpret_cast<T *>(ExecContext::getGlobalContext().bm_.fixX(pid));
    ptr->dirty = true;
  }

  explicit GuardX(GuardO<T> &&other) {
    assert(other.pid != moved);
    for (u64 repeatCounter = 0;; repeatCounter++) {
      PageState &ps =
          ExecContext::getGlobalContext().bm_.getPageState(other.pid);
      u64 stateAndVersion = ps.stateAndVersion;
      if ((stateAndVersion << 8) != (other.version << 8))
        throw OLCRestartException();
      u64 state = PageState::getState(stateAndVersion);
      if ((state == PageState::Unlocked) || (state == PageState::Marked)) {
        if (ps.tryLockX(stateAndVersion)) {
          pid = other.pid;
          ptr = other.ptr;
          ptr->dirty = true;
          other.pid = moved;
          other.ptr = nullptr;
          return;
        }
      }
      yield(repeatCounter);
    }
  }

  // assignment operator
  GuardX &operator=(const GuardX &) = delete;

  // move assignment operator
  GuardX &operator=(GuardX &&other) {
    if (pid != moved) {
      ExecContext::getGlobalContext().bm_.unfixX(pid);
    }
    pid = other.pid;
    ptr = other.ptr;
    other.pid = moved;
    other.ptr = nullptr;
    return *this;
  }

  // copy constructor
  GuardX(const GuardX &) = delete;

  // destructor
  ~GuardX() {
    if (pid != moved)
      ExecContext::getGlobalContext().bm_.unfixX(pid);
  }

  T *operator->() {
    assert(pid != moved);
    return ptr;
  }

  void release() {
    if (pid != moved) {
      ExecContext::getGlobalContext().bm_.unfixX(pid);
      pid = moved;
    }
  }
};

template <class T> struct AllocGuard : public GuardX<T> {
  template <typename... Params> AllocGuard(Params &&...params) {
    GuardX<T>::ptr =
        reinterpret_cast<T *>(ExecContext::getGlobalContext().bm_.allocPage());
    new (GuardX<T>::ptr) T(std::forward<Params>(params)...);
    GuardX<T>::pid = ExecContext::getGlobalContext().bm_.toPID(GuardX<T>::ptr);
  }
};

template <class T> struct GuardS {
  PID pid;
  T *ptr;
  static const u64 moved = ~0ull;

  // constructor
  explicit GuardS(u64 pid) : pid(pid) {
    ptr = reinterpret_cast<T *>(ExecContext::getGlobalContext().bm_.fixS(pid));
  }

  GuardS(GuardO<T> &&other) {
    assert(other.pid != moved);
    if (ExecContext::getGlobalContext().bm_.getPageState(other.pid).tryLockS(
            other.version)) { // XXX: optimize?
      pid = other.pid;
      ptr = other.ptr;
      other.pid = moved;
      other.ptr = nullptr;
    } else {
      throw OLCRestartException();
    }
  }

  GuardS(GuardS &&other) {
    if (pid != moved)
      ExecContext::getGlobalContext().bm_.unfixS(pid);
    pid = other.pid;
    ptr = other.ptr;
    other.pid = moved;
    other.ptr = nullptr;
  }

  // assignment operator
  GuardS &operator=(const GuardS &) = delete;

  // move assignment operator
  GuardS &operator=(GuardS &&other) {
    if (pid != moved)
      ExecContext::getGlobalContext().bm_.unfixS(pid);
    pid = other.pid;
    ptr = other.ptr;
    other.pid = moved;
    other.ptr = nullptr;
    return *this;
  }

  // copy constructor
  GuardS(const GuardS &) = delete;

  // destructor
  ~GuardS() {
    if (pid != moved)
      ExecContext::getGlobalContext().bm_.unfixS(pid);
  }

  T *operator->() {
    assert(pid != moved);
    return ptr;
  }

  void release() {
    if (pid != moved) {
      ExecContext::getGlobalContext().bm_.unfixS(pid);
      pid = moved;
    }
  }
};

inline u64 envOr(const char *env, u64 value) {
  if (getenv(env))
    return atof(getenv(env));
  return value;
}

//---------------------------------------------------------------------------

struct BTreeNode;

struct BTreeNodeHeader {
  static const unsigned underFullSize =
      (pageSize / 2) + (pageSize / 4); // merge nodes more empty
  static const u64 noNeighbour = ~0ull;

  struct FenceKeySlot {
    u16 offset;
    u16 len;
  };

  bool dirty;
  union {
    PID upperInnerNode;             // inner
    PID nextLeafNode = noNeighbour; // leaf
  };

  bool hasRightNeighbour() { return nextLeafNode != noNeighbour; }

  FenceKeySlot lowerFence = {0, 0}; // exclusive
  FenceKeySlot upperFence = {0, 0}; // inclusive

  bool hasLowerFence() { return !!lowerFence.len; };

  u16 count = 0;
  bool isLeaf;
  u16 spaceUsed = 0;
  u16 dataOffset = static_cast<u16>(pageSize);
  u16 prefixLen = 0;

  static const unsigned hintCount = 16;
  u32 hint[hintCount];
  u32 padding;

  BTreeNodeHeader(bool isLeaf) : isLeaf(isLeaf) {}
  ~BTreeNodeHeader() {}
};

static unsigned min(unsigned a, unsigned b) { return a < b ? a : b; }

template <class T> static T loadUnaligned(void *p) {
  T x;
  memcpy(&x, p, sizeof(T));
  return x;
}

// Get order-preserving head of key (assuming little endian)
static u32 head(u8 *key, unsigned keyLen) {
  switch (keyLen) {
  case 0:
    return 0;
  case 1:
    return static_cast<u32>(key[0]) << 24;
  case 2:
    return static_cast<u32>(__builtin_bswap16(loadUnaligned<u16>(key))) << 16;
  case 3:
    return (static_cast<u32>(__builtin_bswap16(loadUnaligned<u16>(key)))
            << 16) |
           (static_cast<u32>(key[2]) << 8);
  default:
    return __builtin_bswap32(loadUnaligned<u32>(key));
  }
}

struct BTreeNode : public BTreeNodeHeader {
  struct Slot {
    u16 offset;
    u16 keyLen;
    u16 payloadLen;
    union {
      u32 head;
      u8 headBytes[4];
    };
  } __attribute__((packed));
  union {
    Slot slot[(pageSize - sizeof(BTreeNodeHeader)) /
              sizeof(Slot)];                     // grows from front
    u8 heap[pageSize - sizeof(BTreeNodeHeader)]; // grows from back
  };

  static constexpr unsigned maxKVSize =
      ((pageSize - sizeof(BTreeNodeHeader) - (2 * sizeof(Slot)))) / 4;

  BTreeNode(bool isLeaf) : BTreeNodeHeader(isLeaf) { dirty = true; }

  u8 *ptr() { return reinterpret_cast<u8 *>(this); }
  bool isInner() { return !isLeaf; }
  span<u8> getLowerFence() {
    return {ptr() + lowerFence.offset, lowerFence.len};
  }
  span<u8> getUpperFence() {
    return {ptr() + upperFence.offset, upperFence.len};
  }
  u8 *getPrefix() { return ptr() + lowerFence.offset; } // any key on page is ok

  unsigned freeSpace() {
    return dataOffset - (reinterpret_cast<u8 *>(slot + count) - ptr());
  }
  unsigned freeSpaceAfterCompaction() {
    return pageSize - (reinterpret_cast<u8 *>(slot + count) - ptr()) -
           spaceUsed;
  }

  bool hasSpaceFor(unsigned keyLen, unsigned payloadLen) {
    return spaceNeeded(keyLen, payloadLen) <= freeSpaceAfterCompaction();
  }

  u8 *getKey(unsigned slotId) { return ptr() + slot[slotId].offset; }
  span<u8> getPayload(unsigned slotId) {
    return {ptr() + slot[slotId].offset + slot[slotId].keyLen,
            slot[slotId].payloadLen};
  }

  PID getChild(unsigned slotId) {
    return loadUnaligned<PID>(getPayload(slotId).data());
  }

  // How much space would inserting a new key of len "keyLen" require?
  unsigned spaceNeeded(unsigned keyLen, unsigned payloadLen) {
    return sizeof(Slot) + (keyLen - prefixLen) + payloadLen;
  }

  void makeHint() {
    unsigned dist = count / (hintCount + 1);
    for (unsigned i = 0; i < hintCount; i++)
      hint[i] = slot[dist * (i + 1)].head;
  }

  void updateHint(unsigned slotId) {
    unsigned dist = count / (hintCount + 1);
    unsigned begin = 0;
    if ((count > hintCount * 2 + 1) &&
        (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
      begin = (slotId / dist) - 1;
    for (unsigned i = begin; i < hintCount; i++)
      hint[i] = slot[dist * (i + 1)].head;
  }

  void searchHint(u32 keyHead, u16 &lowerOut, u16 &upperOut) {
    if (count > hintCount * 2) {
      u16 dist = upperOut / (hintCount + 1);
      u16 pos, pos2;
      for (pos = 0; pos < hintCount; pos++)
        if (hint[pos] >= keyHead)
          break;
      for (pos2 = pos; pos2 < hintCount; pos2++)
        if (hint[pos2] != keyHead)
          break;
      lowerOut = pos * dist;
      if (pos2 < hintCount)
        upperOut = (pos2 + 1) * dist;
    }
  }

  // lower bound search, foundExactOut indicates if there is an exact match,
  // returns slotId
  u16 lowerBound(span<u8> skey, bool &foundExactOut) {
    foundExactOut = false;

    // check prefix
    int cmp = memcmp(skey.data(), getPrefix(), min(skey.size(), prefixLen));
    if (cmp < 0) // key is less than prefix
      return 0;
    if (cmp > 0) // key is greater than prefix
      return count;
    if (skey.size() < prefixLen) // key is equal but shorter than prefix
      return 0;
    u8 *key = skey.data() + prefixLen;
    unsigned keyLen = skey.size() - prefixLen;

    // check hint
    u16 lower = 0;
    u16 upper = count;
    u32 keyHead = head(key, keyLen);
    searchHint(keyHead, lower, upper);

    // binary search on remaining range
    while (lower < upper) {
      u16 mid = ((upper - lower) / 2) + lower;
      if (keyHead < slot[mid].head) {
        upper = mid;
      } else if (keyHead > slot[mid].head) {
        lower = mid + 1;
      } else { // head is equal, check full key
        int cmp = memcmp(key, getKey(mid), min(keyLen, slot[mid].keyLen));
        if (cmp < 0) {
          upper = mid;
        } else if (cmp > 0) {
          lower = mid + 1;
        } else {
          if (keyLen < slot[mid].keyLen) { // key is shorter
            upper = mid;
          } else if (keyLen > slot[mid].keyLen) { // key is longer
            lower = mid + 1;
          } else {
            foundExactOut = true;
            return mid;
          }
        }
      }
    }
    return lower;
  }

  // lowerBound wrapper ignoring exact match argument (for convenience)
  u16 lowerBound(span<u8> key) {
    bool ignore;
    return lowerBound(key, ignore);
  }

  // insert key/value pair
  void insertInPage(span<u8> key, span<u8> payload) {
    unsigned needed = spaceNeeded(key.size(), payload.size());
    if (needed > freeSpace()) {
      assert(needed <= freeSpaceAfterCompaction());
      compactify();
    }
    unsigned slotId = lowerBound(key);
    memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
    storeKeyValue(slotId, key, payload);
    count++;
    updateHint(slotId);
  }

  bool removeSlot(unsigned slotId) {
    spaceUsed -= slot[slotId].keyLen;
    spaceUsed -= slot[slotId].payloadLen;
    memmove(slot + slotId, slot + slotId + 1,
            sizeof(Slot) * (count - slotId - 1));
    count--;
    makeHint();
    return true;
  }

  bool removeInPage(span<u8> key) {
    bool found;
    unsigned slotId = lowerBound(key, found);
    if (!found)
      return false;
    return removeSlot(slotId);
  }

  void copyNode(BTreeNodeHeader *dst, BTreeNodeHeader *src) {
    u64 ofs = offsetof(BTreeNodeHeader, upperInnerNode);
    memcpy(reinterpret_cast<u8 *>(dst) + ofs, reinterpret_cast<u8 *>(src) + ofs,
           sizeof(BTreeNode) - ofs);
  }

  void compactify() {
    unsigned should = freeSpaceAfterCompaction();
    static_cast<void>(should);
    BTreeNode tmp(isLeaf);
    tmp.setFences(getLowerFence(), getUpperFence());
    copyKeyValueRange(&tmp, 0, 0, count);
    tmp.upperInnerNode = upperInnerNode;
    copyNode(this, &tmp);
    makeHint();
    assert(freeSpace() == should);
  }

  // merge right node into this node
  bool mergeNodes(unsigned slotId, BTreeNode *parent, BTreeNode *right) {
    if (!isLeaf)
      // TODO: implement inner merge
      return true;

    assert(right->isLeaf);
    assert(parent->isInner());
    BTreeNode tmp(isLeaf);
    tmp.setFences(getLowerFence(), right->getUpperFence());
    unsigned leftGrow = (prefixLen - tmp.prefixLen) * count;
    unsigned rightGrow = (right->prefixLen - tmp.prefixLen) * right->count;
    unsigned spaceUpperBound =
        spaceUsed + right->spaceUsed +
        (reinterpret_cast<u8 *>(slot + count + right->count) - ptr()) +
        leftGrow + rightGrow;
    if (spaceUpperBound > pageSize)
      return false;
    copyKeyValueRange(&tmp, 0, 0, count);
    right->copyKeyValueRange(&tmp, count, 0, right->count);
    PID pid = ExecContext::getGlobalContext().bm_.toPID(this);
    memcpy(parent->getPayload(slotId + 1).data(), &pid, sizeof(PID));
    parent->removeSlot(slotId);
    tmp.makeHint();
    tmp.nextLeafNode = right->nextLeafNode;

    copyNode(this, &tmp);
    return true;
  }

  // store key/value pair at slotId
  void storeKeyValue(u16 slotId, span<u8> skey, span<u8> payload) {
    // slot
    u8 *key = skey.data() + prefixLen;
    unsigned keyLen = skey.size() - prefixLen;
    slot[slotId].head = head(key, keyLen);
    slot[slotId].keyLen = keyLen;
    slot[slotId].payloadLen = payload.size();
    // key
    unsigned space = keyLen + payload.size();
    dataOffset -= space;
    spaceUsed += space;
    slot[slotId].offset = dataOffset;
    assert(getKey(slotId) >= reinterpret_cast<u8 *>(&slot[slotId]));
    memcpy(getKey(slotId), key, keyLen);
    memcpy(getPayload(slotId).data(), payload.data(), payload.size());
  }

  void copyKeyValueRange(BTreeNode *dst, u16 dstSlot, u16 srcSlot,
                         unsigned srcCount) {
    if (prefixLen <= dst->prefixLen) { // prefix grows
      unsigned diff = dst->prefixLen - prefixLen;
      for (unsigned i = 0; i < srcCount; i++) {
        unsigned newKeyLen = slot[srcSlot + i].keyLen - diff;
        unsigned space = newKeyLen + slot[srcSlot + i].payloadLen;
        dst->dataOffset -= space;
        dst->spaceUsed += space;
        dst->slot[dstSlot + i].offset = dst->dataOffset;
        u8 *key = getKey(srcSlot + i) + diff;
        memcpy(dst->getKey(dstSlot + i), key, space);
        dst->slot[dstSlot + i].head = head(key, newKeyLen);
        dst->slot[dstSlot + i].keyLen = newKeyLen;
        dst->slot[dstSlot + i].payloadLen = slot[srcSlot + i].payloadLen;
      }
    } else {
      for (unsigned i = 0; i < srcCount; i++)
        copyKeyValue(srcSlot + i, dst, dstSlot + i);
    }
    dst->count += srcCount;
    assert((dst->ptr() + dst->dataOffset) >=
           reinterpret_cast<u8 *>(dst->slot + dst->count));
  }

  void copyKeyValue(u16 srcSlot, BTreeNode *dst, u16 dstSlot) {
    unsigned fullLen = slot[srcSlot].keyLen + prefixLen;
    u8 key[fullLen];
    memcpy(key, getPrefix(), prefixLen);
    memcpy(key + prefixLen, getKey(srcSlot), slot[srcSlot].keyLen);
    dst->storeKeyValue(dstSlot, {key, fullLen}, getPayload(srcSlot));
  }

  void insertFence(FenceKeySlot &fk, span<u8> key) {
    assert(freeSpace() >= key.size());
    dataOffset -= key.size();
    spaceUsed += key.size();
    fk.offset = dataOffset;
    fk.len = key.size();
    memcpy(ptr() + dataOffset, key.data(), key.size());
  }

  void setFences(span<u8> lower, span<u8> upper) {
    insertFence(lowerFence, lower);
    insertFence(upperFence, upper);
    for (prefixLen = 0; (prefixLen < min(lower.size(), upper.size())) &&
                        (lower[prefixLen] == upper[prefixLen]);
         prefixLen++)
      ;
  }

  void splitNode(BTreeNode *parent, unsigned sepSlot, span<u8> sep) {
    assert(sepSlot > 0);
    assert(sepSlot < (pageSize / sizeof(PID)));

    BTreeNode tmp(isLeaf);
    BTreeNode *nodeLeft = &tmp;

    AllocGuard<BTreeNode> newNode(isLeaf);
    BTreeNode *nodeRight = newNode.ptr;

    nodeLeft->setFences(getLowerFence(), sep);
    nodeRight->setFences(sep, getUpperFence());

    PID leftPID = ExecContext::getGlobalContext().bm_.toPID(this);
    u16 oldParentSlot = parent->lowerBound(sep);
    if (oldParentSlot == parent->count) {
      assert(parent->upperInnerNode == leftPID);
      parent->upperInnerNode = newNode.pid;
    } else {
      assert(parent->getChild(oldParentSlot) == leftPID);
      memcpy(parent->getPayload(oldParentSlot).data(), &newNode.pid,
             sizeof(PID));
    }
    parent->insertInPage(sep, {reinterpret_cast<u8 *>(&leftPID), sizeof(PID)});

    if (isLeaf) {
      copyKeyValueRange(nodeLeft, 0, 0, sepSlot + 1);
      copyKeyValueRange(nodeRight, 0, nodeLeft->count, count - nodeLeft->count);
      nodeLeft->nextLeafNode = newNode.pid;
      nodeRight->nextLeafNode = this->nextLeafNode;
    } else {
      // in inner node split, separator moves to parent (count == 1 +
      // nodeLeft->count + nodeRight->count)
      copyKeyValueRange(nodeLeft, 0, 0, sepSlot);
      copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1,
                        count - nodeLeft->count - 1);
      nodeLeft->upperInnerNode = getChild(nodeLeft->count);
      nodeRight->upperInnerNode = upperInnerNode;
    }
    nodeLeft->makeHint();
    nodeRight->makeHint();
    copyNode(this, nodeLeft);
  }

  struct SeparatorInfo {
    unsigned len;     // len of new separator
    unsigned slot;    // slot at which we split
    bool isTruncated; // if true, we truncate the separator taking len bytes
                      // from slot+1
  };

  unsigned commonPrefix(unsigned slotA, unsigned slotB) {
    assert(slotA < count);
    unsigned limit = min(slot[slotA].keyLen, slot[slotB].keyLen);
    u8 *a = getKey(slotA), *b = getKey(slotB);
    unsigned i;
    for (i = 0; i < limit; i++)
      if (a[i] != b[i])
        break;
    return i;
  }

  SeparatorInfo findSeparator(bool splitOrdered) {
    assert(count > 1);
    if (isInner()) {
      // inner nodes are split in the middle
      unsigned slotId = count / 2;
      return SeparatorInfo{
          static_cast<unsigned>(prefixLen + slot[slotId].keyLen), slotId,
          false};
    }

    // find good separator slot
    unsigned bestPrefixLen, bestSlot;

    if (splitOrdered) {
      bestSlot = count - 2;
    } else if (count > 16) {
      unsigned lower = (count / 2) - (count / 16);
      unsigned upper = (count / 2);

      bestPrefixLen = commonPrefix(lower, 0);
      bestSlot = lower;

      if (bestPrefixLen != commonPrefix(upper - 1, 0))
        for (bestSlot = lower + 1;
             (bestSlot < upper) && (commonPrefix(bestSlot, 0) == bestPrefixLen);
             bestSlot++)
          ;
    } else {
      bestSlot = (count - 1) / 2;
    }

    // try to truncate separator
    unsigned common = commonPrefix(bestSlot, bestSlot + 1);
    if ((bestSlot + 1 < count) && (slot[bestSlot].keyLen > common) &&
        (slot[bestSlot + 1].keyLen > (common + 1)))
      return SeparatorInfo{prefixLen + common + 1, bestSlot, true};

    return SeparatorInfo{
        static_cast<unsigned>(prefixLen + slot[bestSlot].keyLen), bestSlot,
        false};
  }

  void getSep(u8 *sepKeyOut, SeparatorInfo info) {
    memcpy(sepKeyOut, getPrefix(), prefixLen);
    memcpy(sepKeyOut + prefixLen, getKey(info.slot + info.isTruncated),
           info.len - prefixLen);
  }

  PID lookupInner(span<u8> key) {
    unsigned pos = lowerBound(key);
    if (pos == count)
      return upperInnerNode;
    return getChild(pos);
  }
};

static_assert(sizeof(BTreeNode) == pageSize, "btree node size problem");

static const u64 metadataPageId = 0;

struct MetaDataPage {
  bool dirty;
  PID roots[(pageSize - 8) / 8];

  PID getRoot(unsigned slot) { return roots[slot]; }
};

struct BTree {
private:
  void trySplit(GuardX<BTreeNode> &&node, GuardX<BTreeNode> &&parent,
                span<u8> key, unsigned payloadLen);
  void ensureSpace(BTreeNode *toSplit, span<u8> key, unsigned payloadLen);

public:
  unsigned slotId;
  atomic<bool> splitOrdered;

  BTree();
  ~BTree();

  GuardO<BTreeNode> findLeafO(span<u8> key) {
    GuardO<MetaDataPage> meta(metadataPageId);
    GuardO<BTreeNode> node(meta->getRoot(slotId), meta);
    meta.release();

    while (node->isInner())
      node = GuardO<BTreeNode>(node->lookupInner(key), node);
    return node;
  }

  // point lookup, returns payload len on success, or -1 on failure
  int lookup(span<u8> key, u8 *payloadOut, unsigned payloadOutSize) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
      try {
        GuardO<BTreeNode> node = findLeafO(key);
        bool found;
        unsigned pos = node->lowerBound(key, found);
        if (!found)
          return -1;

        // key found, copy payload
        memcpy(payloadOut, node->getPayload(pos).data(),
               min(node->slot[pos].payloadLen, payloadOutSize));
        return node->slot[pos].payloadLen;
      } catch (const OLCRestartException &) {
        yield(repeatCounter);
      }
    }
  }

  template <class Fn> bool lookup(span<u8> key, Fn fn) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
      try {
        GuardO<BTreeNode> node = findLeafO(key);
        bool found;
        unsigned pos = node->lowerBound(key, found);
        if (!found)
          return false;

        // key found
        fn(node->getPayload(pos));
        return true;
      } catch (const OLCRestartException &) {
        yield(repeatCounter);
      }
    }
  }

  void insert(span<u8> key, span<u8> payload);
  bool remove(span<u8> key);

  template <class Fn> bool updateInPlace(span<u8> key, Fn fn) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
      try {
        GuardO<BTreeNode> node = findLeafO(key);
        bool found;
        unsigned pos = node->lowerBound(key, found);
        if (!found)
          return false;

        {
          GuardX<BTreeNode> nodeLocked(move(node));
          fn(nodeLocked->getPayload(pos));
          return true;
        }
      } catch (const OLCRestartException &) {
        yield(repeatCounter);
      }
    }
  }

  GuardS<BTreeNode> findLeafS(span<u8> key) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
      try {
        GuardO<MetaDataPage> meta(metadataPageId);
        GuardO<BTreeNode> node(meta->getRoot(slotId), meta);
        meta.release();

        while (node->isInner())
          node = GuardO<BTreeNode>(node->lookupInner(key), node);

        return GuardS<BTreeNode>(move(node));
      } catch (const OLCRestartException &) {
        yield(repeatCounter);
      }
    }
  }

  template <class Fn> void scanAsc(span<u8> key, Fn fn) {
    GuardS<BTreeNode> node = findLeafS(key);
    bool found;
    unsigned pos = node->lowerBound(key, found);
    for (u64 repeatCounter = 0;; repeatCounter++) { // XXX
      if (pos < node->count) {
        if (!fn(*node.ptr, pos))
          return;
        pos++;
      } else {
        if (!node->hasRightNeighbour())
          return;
        pos = 0;
        node = GuardS<BTreeNode>(node->nextLeafNode);
      }
    }
  }

  template <class Fn> void scanDesc(span<u8> key, Fn fn) {
    GuardS<BTreeNode> node = findLeafS(key);
    bool exactMatch;
    int pos = node->lowerBound(key, exactMatch);
    if (pos == node->count) {
      pos--;
      exactMatch = true; // XXX:
    }
    for (u64 repeatCounter = 0;; repeatCounter++) { // XXX
      while (pos >= 0) {
        if (!fn(*node.ptr, pos, exactMatch))
          return;
        pos--;
      }
      if (!node->hasLowerFence())
        return;
      node = findLeafS(node->getLowerFence());
      pos = node->count - 1;
    }
  }
};

static unsigned btreeslotcounter = 0;