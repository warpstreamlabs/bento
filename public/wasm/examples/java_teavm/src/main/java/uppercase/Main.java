package uppercase;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.teavm.interop.Export;
import org.teavm.interop.Function;

public class Main {
    abstract static class Callback extends Function {
        abstract long call(long batch);
    }

    static Callback processBatchRef;

    // Field numbers from resources/protos/message.proto.
    private static final int BATCHES_BATCHES = 1;
    private static final int BATCH_PARTS = 1;
    private static final int PART_RAW = 1;

    public static void main(String[] args) {
        // TeaVM's C backend only keeps code reachable from main, so reference the
        // export to stop it being removed.
        processBatchRef = Function.get(Callback.class, Main.class, "processBatch");
    }

    /** Uppercases the raw content of each message of a Batch, returning Batches. */
    @Export(name = "java_process_batch")
    public static long processBatch(long batch) {
        ByteArrayOutputStream parts = new ByteArrayOutputStream();
        Proto in = new Proto(Bento.read(batch));
        while (in.more()) {
            in.field();
            Proto.write(parts, BATCH_PARTS, uppercase(in.bytes()));
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Proto.write(out, BATCHES_BATCHES, parts.toByteArray());
        return Bento.output(out.toByteArray());
    }

    /** Uppercases the raw content of a Part, and copies its other fields. */
    private static byte[] uppercase(byte[] part) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Proto in = new Proto(part);
        while (in.more()) {
            int field = in.field();
            byte[] value = in.bytes();
            if (field == PART_RAW) {
                value = new String(value, StandardCharsets.UTF_8).toUpperCase().getBytes(StandardCharsets.UTF_8);
            }
            Proto.write(out, field, value);
        }
        return out.toByteArray();
    }
}
