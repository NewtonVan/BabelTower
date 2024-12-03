#pragma once
#include <buffer/vm_buffer.hpp>
#include <common/exec_context.hpp>

struct OLCRestartException {};

template <class T> struct GuardO {
  PID pid;
  T *ptr;
  u64 version;
  static const u64 moved = ~0ull;

  // constructor
  explicit GuardO(u64 pid)
      : pid(pid), ptr(reinterpret_cast<T *>(
                      ExecContext::getGlobalContext().bm_->toPtr(pid))) {
    init();
  }

  template <class T2> GuardO(u64 pid, GuardO<T2> &parent) {
    parent.checkVersionAndRestart();
    this->pid = pid;
    ptr =
        reinterpret_cast<T *>(ExecContext::getGlobalContext().bm_->toPtr(pid));
    init();
  }

  GuardO(GuardO &&other) {
    pid = other.pid;
    ptr = other.ptr;
    version = other.version;
  }

  void init() {
    assert(pid != moved);
    PageState &ps = ExecContext::getGlobalContext().bm_->getPageState(pid);
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
          ExecContext::getGlobalContext().bm_->handleFault(pid);
          ExecContext::getGlobalContext().bm_->unfixX(pid);
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
      PageState &ps = ExecContext::getGlobalContext().bm_->getPageState(pid);
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
    ptr = reinterpret_cast<T *>(ExecContext::getGlobalContext().bm_->fixX(pid));
    ptr->dirty = true;
  }

  explicit GuardX(GuardO<T> &&other) {
    assert(other.pid != moved);
    for (u64 repeatCounter = 0;; repeatCounter++) {
      PageState &ps =
          ExecContext::getGlobalContext().bm_->getPageState(other.pid);
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
      ExecContext::getGlobalContext().bm_->unfixX(pid);
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
      ExecContext::getGlobalContext().bm_->unfixX(pid);
  }

  T *operator->() {
    assert(pid != moved);
    return ptr;
  }

  void release() {
    if (pid != moved) {
      ExecContext::getGlobalContext().bm_->unfixX(pid);
      pid = moved;
    }
  }
};

template <class T> struct AllocGuard : public GuardX<T> {
  template <typename... Params> AllocGuard(Params &&...params) {
    GuardX<T>::ptr =
        reinterpret_cast<T *>(ExecContext::getGlobalContext().bm_->allocPage());
    new (GuardX<T>::ptr) T(std::forward<Params>(params)...);
    GuardX<T>::pid = ExecContext::getGlobalContext().bm_->toPID(GuardX<T>::ptr);
  }
};

template <class T> struct GuardS {
  PID pid;
  T *ptr;
  static const u64 moved = ~0ull;

  // constructor
  explicit GuardS(u64 pid) : pid(pid) {
    ptr = reinterpret_cast<T *>(ExecContext::getGlobalContext().bm_->fixS(pid));
  }

  GuardS(GuardO<T> &&other) {
    assert(other.pid != moved);
    if (ExecContext::getGlobalContext().bm_->getPageState(other.pid).tryLockS(
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
      ExecContext::getGlobalContext().bm_->unfixS(pid);
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
      ExecContext::getGlobalContext().bm_->unfixS(pid);
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
      ExecContext::getGlobalContext().bm_->unfixS(pid);
  }

  T *operator->() {
    assert(pid != moved);
    return ptr;
  }

  void release() {
    if (pid != moved) {
      ExecContext::getGlobalContext().bm_->unfixS(pid);
      pid = moved;
    }
  }
};