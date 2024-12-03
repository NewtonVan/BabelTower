#pragma once
#include <common/type.hpp>
#include <cstdlib>
inline u64 envOr(const char *env, u64 value) {
  if (getenv(env))
    return atof(getenv(env));
  return value;
}