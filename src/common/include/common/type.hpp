#pragma once
#include <cstdint>

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef u64 PID; // page id type

namespace bustub {
static constexpr int INVALID_PAGE_ID = -1;    // invalid page id
static constexpr int INVALID_TXN_ID = -1;     // invalid transaction id
static constexpr int INVALID_LSN = -1;        // invalid log sequence number
static constexpr int BUSTUB_PAGE_SIZE = 4096; // size of a data page in byte
static constexpr int BUFFER_POOL_SIZE = 10;   // size of buffer pool
static constexpr int LOG_BUFFER_SIZE =
    ((BUFFER_POOL_SIZE + 1) * BUSTUB_PAGE_SIZE); // size of a log buffer in byte
static constexpr int LRUK_REPLACER_K = 10; // lookback window for lru-k replacer

using frame_id_t = int32_t; // frame id type
using page_id_t = int32_t;  // page id type
using txn_id_t = int32_t;   // transaction id type
using lsn_t = int32_t;      // log sequence number type
} // namespace bustub