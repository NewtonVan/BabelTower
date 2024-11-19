#include <tpcc/ThreadContext.hpp>

__thread uint16_t workerThreadId = 0;
__thread int32_t tpcchistorycounter = 0;