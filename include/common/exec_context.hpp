#pragma once
#include <buffer/vm_buffer.hpp>

struct ExecContext {
  // TODO(chen): use a uniq ptr?
  BufferManager bm_;
  static ExecContext &getGlobalContext();

private:
  static ExecContext global_ctx;
};