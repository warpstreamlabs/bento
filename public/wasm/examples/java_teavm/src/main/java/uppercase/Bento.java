package uppercase;

import org.teavm.interop.Address;
import org.teavm.interop.Import;
import org.teavm.interop.c.Include;

/**
 * Exchanges buffers with Bento (interop/bento.h). Buffers pack an address in
 * their upper 32 bits and a length in their lower 32 bits.
 */
final class Bento {
    private Bento() {
    }

    @Import(name = "bento_retain")
    @Include("bento.h")
    private static native long retain(long data);

    /** Copies the input of a call, which Bento releases once the call returns. */
    static byte[] read(long buffer) {
        byte[] data = new byte[(int) buffer];
        Address.moveMemoryBlock(Address.fromInt((int) (buffer >>> 32)), Address.ofData(data), data.length);
        return data;
    }

    /** Copies the output of a call into a buffer outside of the Java heap. */
    static long output(byte[] data) {
        // The GC can't move the array while it's copied.
        long buffer = data.length == 0 ? 0 : (long) Address.ofData(data).toInt() << 32 | data.length;
        return retain(buffer);
    }
}
