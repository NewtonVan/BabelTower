#include <common/exec_context.hpp>

ExecContext ExecContext::global_ctx;

ExecContext &ExecContext::getGlobalContext() { return global_ctx; }