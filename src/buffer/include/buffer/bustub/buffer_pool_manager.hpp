#pragma once
#include "buffer/bustub/log_manager.hpp"
#include <list>
#include <mutex>
#include <queue>
#include <unordered_map>

#include <buffer/buffer_manager.hpp>
#include <buffer/bustub/disk_scheduler.hpp>
#include <buffer/bustub/lru_k_replacer.hpp>
#include <common/type.hpp>
#include <storage/page/page_guard.hpp>

namespace bustub {
// TODO: rm stub class
struct LogManager;

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManager : IBufferManager {
public:
  /**
   * @brief Creates a new BufferPoolManager.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param replacer_k the lookback constant k for the LRU-K replacer
   * @param log_manager the log manager (for testing only: nullptr = disable
   * logging). Please ignore this for P1.
   */
  BufferPoolManager(size_t pool_size, DiskManager *disk_manager,
                    size_t replacer_k = LRUK_REPLACER_K,
                    LogManager *log_manager = nullptr);

  /**
   * @brief Destroy an existing BufferPoolManager.
   */
  ~BufferPoolManager();

  /** @brief Return the size (number of frames) of the buffer pool. */
  auto GetPoolSize() -> size_t { return pool_size_; }

  /** @brief Return the pointer to all the pages in the buffer pool. */
  auto GetPages() -> Page * { return pages_; }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Create a new page in the buffer pool. Set page_id to the new page's
   * id, or nullptr if all frames are currently in use and not evictable (in
   * another word, pinned).
   *
   * You should pick the replacement frame from either the free list or the
   * replacer (always find from the free list first), and then call the
   * AllocatePage() method to get a new page id. If the replacement frame has a
   * dirty page, you should write it back to the disk first. You also need to
   * reset the memory and metadata for the new page.
   *
   * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id,
   * false) so that the replacer wouldn't evict the frame before the buffer pool
   * manager "Unpin"s it. Also, remember to record the access history of the
   * frame in the replacer for the lru-k algorithm to work.
   *
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new
   * page
   */
  auto NewPage(page_id_t *page_id) -> Page *;

  /**
   * TODO(P1): Add implementation
   *
   * @brief PageGuard wrapper for NewPage
   *
   * Functionality should be the same as NewPage, except that
   * instead of returning a pointer to a page, you return a
   * BasicPageGuard structure.
   *
   * @param[out] page_id, the id of the new page
   * @return BasicPageGuard holding a new page
   */
  auto NewPageGuarded(page_id_t *page_id) -> BasicPageGuard;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Fetch the requested page from the buffer pool. Return nullptr if
   * page_id needs to be fetched from the disk but all frames are currently in
   * use and not evictable (in another word, pinned).
   *
   * First search for page_id in the buffer pool. If not found, pick a
   * replacement frame from either the free list or the replacer (always find
   * from the free list first), read the page from disk by calling
   * disk_manager_->ReadPage(), and replace the old page in the frame. Similar
   * to NewPage(), if the old page is dirty, you need to write it back to disk
   * and update the metadata of the new page
   *
   * In addition, remember to disable eviction and record the access history of
   * the frame like you did for NewPage().
   *
   * @param page_id id of page to be fetched
   * @param access_type type of access to the page, only needed for leaderboard
   * tests.
   * @return nullptr if page_id cannot be fetched, otherwise pointer to the
   * requested page
   */
  auto FetchPage(page_id_t page_id,
                 AccessType access_type = AccessType::Unknown) -> Page *;

  /**
   * TODO(P1): Add implementation
   *
   * @brief PageGuard wrappers for FetchPage
   *
   * Functionality should be the same as FetchPage, except
   * that, depending on the function called, a guard is returned.
   * If FetchPageRead or FetchPageWrite is called, it is expected that
   * the returned page already has a read or write latch held, respectively.
   *
   * @param page_id, the id of the page to fetch
   * @return PageGuard holding the fetched page
   */
  auto FetchPageBasic(page_id_t page_id) -> BasicPageGuard;
  auto FetchPageRead(page_id_t page_id) -> ReadPageGuard;
  auto FetchPageWrite(page_id_t page_id) -> WritePageGuard;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Unpin the target page from the buffer pool. If page_id is not in the
   * buffer pool or its pin count is already 0, return false.
   *
   * Decrement the pin count of a page. If the pin count reaches 0, the frame
   * should be evictable by the replacer. Also, set the dirty flag on the page
   * to indicate if the page was modified.
   *
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @param access_type type of access to the page, only needed for leaderboard
   * tests.
   * @return false if the page is not in the page table or its pin count is <= 0
   * before this call, true otherwise
   */
  auto UnpinPage(page_id_t page_id, bool is_dirty,
                 AccessType access_type = AccessType::Unknown) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Flush the target page to disk.
   *
   * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS
   * of the dirty flag. Unset the dirty flag of the page after flushing.
   *
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true
   * otherwise
   */
  auto FlushPage(page_id_t page_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Flush all the pages in the buffer pool to disk.
   */
  void FlushAllPages();

  /**
   * TODO(P1): Add implementation
   *
   * @brief Delete a page from the buffer pool. If page_id is not in the buffer
   * pool, do nothing and return true. If the page is pinned and cannot be
   * deleted, return false immediately.
   *
   * After deleting the page from the page table, stop tracking the frame in the
   * replacer and add the frame back to the free list. Also, reset the page's
   * memory and metadata. Finally, you should call DeallocatePage() to imitate
   * freeing the page on the disk.
   *
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page
   * didn't exist or deletion succeeded
   */
  auto DeletePage(page_id_t page_id) -> bool;

  // my code here
  enum class AllocateType { Unknown = 0, New, Fetch };

private:
  /** Number of pages in the buffer pool. */
  const size_t pool_size_;
  /** The next page id to be allocated  */
  std::atomic<page_id_t> next_page_id_ = 0;

  /** Array of buffer pool pages. */
  Page *pages_;
  /** Pointer to the disk manager. */
  // DiskManager *disk_manager_ __attribute__((__unused__));
  std::unique_ptr<DiskScheduler> disk_scheduler_ __attribute__((__unused__));
  /** Pointer to the log manager. Please ignore this for P1. */
  LogManager *log_manager_ __attribute__((__unused__));
  /** Page table for keeping track of buffer pool pages. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  /** Replacer to find unpinned pages for replacement. */
  std::unique_ptr<LRUKReplacer> replacer_;
  /** List of free frames that don't have any pages on them. */
  std::list<frame_id_t> free_list_;
  /** This latch protects shared data structures. We recommend updating this
   * comment to describe what it protects. */
  std::mutex latch_;

  /** my member var here */
  std::vector<std::condition_variable> frame_guard_;
  std::vector<bool> ready2read_;

  /**
   * @brief Allocate a page on disk. Caller should acquire the latch before
   * calling this function.
   * @return the id of the allocated page
   */
  auto AllocatePage() -> page_id_t;

  /**
   * @brief Deallocate a page on disk. Caller should acquire the latch before
   * calling this function.
   * @param page_id id of the page to deallocate
   */
  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track
    // deallocated pages
  }

  // TODO(student): You may add additional private members and helper functions
  auto GetPageByFrameId(frame_id_t fid) -> Page * { return pages_ + fid - 1; };

  /** my member func here */
  // assume the caller has already hold the lock
  auto AllocateFrameId() -> std::optional<frame_id_t>;
  auto AllocateFramePage(std::unique_lock<std::mutex> &guard,
                         frame_id_t frame_id, page_id_t page_id,
                         AllocateType allocate_type = AllocateType::Unknown,
                         AccessType access_type = AccessType::Unknown)
      -> Page *;
};
} // namespace bustub
