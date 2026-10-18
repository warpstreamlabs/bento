// WASI has no users, so report "/" as the home directory.
#pragma once
#include <sys/types.h>
struct passwd { char* pw_dir; };
static struct passwd teavm_wasi_passwd = { "/" };
#define getpwuid(uid) (&teavm_wasi_passwd)
#define getuid() 0
