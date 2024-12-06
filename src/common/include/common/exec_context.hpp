#pragma once
#include "buffer/buffer_manager.hpp"
#include <memory>

#include <buffer/vm_buffer.hpp>

struct ExecContext {
  // TODO(chen): use a uniq ptr?
  std::unique_ptr<IBufferManager> bm_;
  static ExecContext &getGlobalContext();
  ExecContext() : bm_(std::make_unique<VMBufferManager>()) {}

private:
  static ExecContext global_ctx;
};