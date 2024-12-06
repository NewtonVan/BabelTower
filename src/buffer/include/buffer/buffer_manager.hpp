#pragma once

#include <buffer/pages.hpp>
#include <common/type.hpp>

using namespace std;

class IBufferManager {
public:
  static const u64 mb = 1024ull * 1024;
  static const u64 gb = 1024ull * 1024 * 1024;
  virtual ~IBufferManager() {}

  virtual Page *fixX(PID pid) = 0;
  virtual void unfixX(PID pid) = 0;
  virtual Page *fixS(PID pid) = 0;
  virtual void unfixS(PID pid) = 0;

  virtual Page *toPtr(PID pid) = 0;
  virtual PID toPID(void *page) = 0;
  virtual PageState &getPageState(PID pid) = 0;
  virtual Page *allocPage() = 0;
  virtual void handleFault(PID pid) = 0;
};
