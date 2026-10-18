// Exports the plugin functions Bento calls, forwarding process_batch to Java
// (Main.java).
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "bento.h"

int teavm_main(int argc, char** argv);
int64_t java_process_batch(int64_t batch);

// Runs as part of the reactor's _initialize, booting the TeaVM runtime.
__attribute__((constructor)) static void boot(void) {
    static char* argv[] = {"plugin", 0};
    teavm_main(1, argv);
}

// Allocates a buffer for Bento to write into, which Bento releases once the
// call returns. This is outside of the Java heap, so the GC never moves it.
__attribute__((export_name("alloc"))) void* bento_alloc(uint32_t size) {
    return size == 0 ? NULL : malloc(size);
}

// Releases a buffer allocated by bento_alloc or returned to Bento.
__attribute__((export_name("release"))) void bento_release(void* ptr, uint32_t len) {
    free(ptr);
}

bento_buffer bento_retain(bento_buffer data) {
    uint32_t len = (uint32_t)data;
    if (len == 0) {
        return 0;
    }
    void* ptr = malloc(len);
    memcpy(ptr, (void*)(uintptr_t)(data >> 32), len);
    return (bento_buffer)(uintptr_t)ptr << 32 | len;
}

// The result area lives as long as the instance. An instance makes one call at a
// time, so one is enough.
static bento_result result;

// A Java exception traps, as the plugin has no other way to report it, so the
// error is never set.
__attribute__((export_name("process_batch"))) bento_result* process_batch(bento_buffer batch) {
    result = (bento_result){.output = (bento_buffer)java_process_batch((int64_t)batch)};
    return &result;
}
