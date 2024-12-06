#include <algorithm>
#include <buffer/bustub/buffer_pool_manager.hpp>
#include <storage/page/page_guard.hpp>
#include <utility>

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (page_ == nullptr) {
    return;
  }

  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept
    -> BasicPageGuard & {
  if (that.page_ == nullptr) {
    return *this;
  }
  if (page_ != nullptr) {
    Drop();
  }

  bpm_ = that.bpm_;
  page_ = that.page_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;

  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  if (bpm_ == nullptr) {
    return;
  }
  Drop();
}; // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
    : guard_(std::move(that.guard_)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept
    -> ReadPageGuard & {
  if (guard_.page_ != nullptr) {
    Drop();
  }

  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }

  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  if (guard_.bpm_ == nullptr) {
    return;
  }
  Drop();
} // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
    : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept
    -> WritePageGuard & {
  if (guard_.page_ != nullptr) {
    Drop();
  }

  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }

  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  if (guard_.bpm_ == nullptr) {
    return;
  }
  Drop();
} // NOLINT

} // namespace bustub
