# Ack `aws_s3_stream` After Durable S3 Part Upload

## Summary

Rework `aws_s3_stream` so upstream messages are not acked when bytes are merely accepted into memory. Instead, ack only after their bytes have been uploaded as an S3 multipart part, with final buffered bytes acked only after `CompleteMultipartUpload`.

This becomes the default behavior. Recovery is S3-only for v1 and supports deterministic `path` values that can be recomputed from redelivered messages. Nondeterministic paths such as `${! uuid_v4() }` are explicitly documented as not crash-recoverable without a future manifest/cache/prefix feature. Duplicates after crash are acceptable.

## Key Changes

- Convert `aws_s3_stream` internally to a transaction-aware output while keeping the same public component name and config surface.
- Preserve existing `batching` behavior by applying the parsed `BatchPolicy` inside the transaction-aware implementation, since returning an internal output bypasses the normal public `BatchOutput` wrapper.
- Store pending ack callbacks, not full transactions, alongside the S3 writer’s byte buffers.
- Keep multipart part boundaries simple: never split a single message; if one message is larger than the target part size, treat it as one part.

## Ack And Upload Semantics

- Bytes in the active in-memory buffer are never acked.
- Once a buffer reaches a valid multipart part threshold, seal it as a whole part. Upload it, but only release its acks after either:
  - a later message for the same deterministic S3 key has been received and is now unacked, or
  - the file is completed successfully on output close.
- This avoids the state where all messages for an incomplete upload have been acked and no future redelivery can recover the exact key.
- On `Close`, upload any remaining sealed/active bytes, call `CompleteMultipartUpload`, and only then ack the final pending callbacks.
- On a part-upload failure, **never abort the multipart upload**. Prior parts are durable on S3 and the upload stays resumable, so aborting would both discard already-acked prior parts (silent data loss) and delete the exact state recovery depends on. Nack only the failing part's callbacks — its bytes are not durable — so they redeliver; leave prior parts, held `pendingAcks`, and the in-progress upload intact. The writer resumes on the next redelivery (transient failures self-heal in-process), or via S3 recovery after a crash.
- On a completion failure during `Close`, likewise do not abort; nack the callbacks still owned by the writer and leave the upload in progress so a restart can recover and complete it.
- Non-final multipart parts must be at least S3’s 5 MiB minimum. `max_buffer_count` and `max_buffer_period` may seal data, but under-5 MiB data is only uploaded as the final part during close.

## Ack Ownership Invariants

A single upstream batch may fan out across multiple partition writers, so these
rules must hold for at-least-once delivery to be correct:

- **Exactly-once resolution.** Every upstream transaction ack is invoked exactly once. Zero invocations leaks the message (stuck in-flight, neither committed nor redelivered); more than one invocation double-resolves it. The combined ack (`s3CombinedAck`) enforces this with a `done` guard so any late or duplicate child ack becomes a no-op.
- **Whole-batch resolution.** A transaction is the unit of acknowledgement and cannot be partially resolved. The combined ack resolves the upstream ack once: nacked on the first partition failure, or acked only once every partition's bytes are durable.
- **Failure nacks the whole batch.** Because resolution is whole-batch, any partition failure nacks the entire transaction. Partitions that already succeeded are redelivered, producing duplicates that are acceptable (consistent with the crash-duplicate tolerance above).
- **Writer ack ownership.** Once a message's ack is handed to a writer via `WriteBytes`, that writer owns resolving it exactly once — including early-return failures (closed or uninitialized writer), which must nack rather than silently drop the ack.

## Recovery Behavior

- On first use of a deterministic S3 key, look for existing in-progress multipart uploads for that exact key using S3 multipart listing APIs.
- If exactly one matching upload exists, recover it with `ListParts`, set the next part number after the highest recovered part, and append new/redelivered data.
- If multiple in-progress uploads exist for the same exact key, fail clearly rather than guessing, to avoid corrupting output.
- If no matching upload exists, start a new multipart upload.
- Recovery does not attempt to identify or resume nondeterministically generated keys unless the next redelivered message recomputes the exact same key.
- Because failures never abort, an interrupted upload is always left in progress for recovery to find — recovery is never defeated by the failure-handling path.

## Sub-5 MiB Deadlock With Bounded Inputs

### Problem

When running `aws_s3_stream` against a **bounded input** (e.g. `generate` with `count: 3`, or an AWS Lambda with SQS), the total output for a given S3 key may stay well under S3's 5 MiB multipart minimum. In that case:

1. Output receives messages into its active buffer. No part is ever sealed mid-stream (buffer never reaches `max_buffer_bytes`).
2. Acks are held in `pendingAcks` / `activeAcks` — they will only be released in `Close()`.
3. `Close()` is only called when the output's `loop()` sees the input's transaction channel close.
4. But a finite input won't close its transaction channel until its in-flight transactions are acked.
5. **Deadlock**: input waits for acks, output waits for channel close, neither progresses.

This was reported in PR #895 (comment, 2026-06-30). It affects any deployment with bounded or self-terminating inputs (Lambda, `generate`, `read_until` with EOF, etc.). On a long-running stream the issue is invisible; on bounded inputs it is a hard hang.

### Resolution

The solution has two parts:

**1. End-of-input as immediate finalize trigger.** When `loop()` detects the transaction channel has closed (the `!open` branch at `output_s3_stream.go:575`), it must flush the batcher if configured, then for every alive `S3StreamingWriter`:
   - Upload any remaining active bytes as the final part (regardless of size).
   - Complete the multipart upload.
   - Ack the final pending callbacks.
   - Close and remove the writer.

This forces the acks to drain before `loop()` returns, breaking the circular wait. The existing `Close()` call on line 579 already does this in principle, but the deadlock arises because the input won't deliver the channel-close signal until its acks return. The fix is to ensure the finalize-and-ack sequence runs *before* the output's own `Close()` is invoked — i.e. the output must not require `Close()` to resolve pending acks. Instead, the `!open` branch should complete and ack everything inline.

**2. PutObject fallback for single-part sub-5 MiB files.** A single-part multipart upload with a < 5 MiB part IS permitted by S3 (the only part is the last part, which has no minimum). The `EntityTooSmall` error only fires when a **non-final** part is < 5 MiB. Nevertheless, `PutObject` is the right approach for three reasons:

   - **Fewer API calls**: `CreateMultipartUpload` + `UploadPart` + `CompleteMultipartUpload` (3 calls) vs `PutObject` (1 call).
   - **Avoids S3-compatible store variance**: MinIO and other non-AWS stores may enforce a blanket 5 MiB minimum on all parts, including the last.
   - **Simpler failure semantics**: `PutObject` is atomic — it either succeeds (object is fully written) or fails (nothing is written), eliminating the "some parts committed, some not" intermediate state that multipart recovery must handle.

   The rule: if a writer has exactly one part and that part is < 5 MiB at finalize time, use `PutObject` instead of `CompleteMultipartUpload`. If the writer has multiple parts (at least one full-sized part already uploaded), complete normally via multipart. The `PutObject` call follows the same "never abort" failure semantics: if it fails, nack the callbacks and leave the writer resumable.

### Test Coverage (additions)

- Integration test with a bounded input (e.g. `generate count: 3`) and no batching, confirming the process exits 0 (no deadlock) and the S3 object is written correctly.
- Integration test confirming `PutObject` is used (rather than multipart) when the final object is < 5 MiB.
- Integration test confirming `CompleteMultipartUpload` is still used when the object has multiple parts (≥ 5 MiB).
- Real-S3 (non-mocked) smoke test for the zero-byte file path with both the `PutObject` path and (for stores that support it) the single-part multipart path.

## Tests

- Unit test that no ack occurs when messages are only in memory.
- Unit test that a sealed/uploaded part is not acked until a later message is owned, then is acked.
- Unit test that final buffered data is acked only after `CompleteMultipartUpload`.
- Unit test that a part-upload failure nacks the failing part's callbacks.
- Unit test that a part-upload failure does not abort: prior (already-acked) parts are preserved, only the failing part is nacked, and the writer resumes to completion.
- Unit test complete failure nacks final pending callbacks.
- Unit test S3 recovery from one exact-key incomplete upload resumes at the next part number.
- Unit test multiple exact-key incomplete uploads returns an error.
- Regression test that configured `batching` still applies.
- Regression test that a multi-partition batch with a failing partition resolves the upstream transaction exactly once (nack), rather than leaking it.
- Unit test that an early `WriteBytes` failure (closed/uninitialized writer) nacks its owned ack.
- Composition test that the output behaves correctly under `fallback`'s consumption contract (async forward + interceptor ack, no per-transaction blocking): no deadlock, a deferred part-upload failure surfaces as a nack, and the writer resumes and completes the file.
- Documentation test/update for deterministic-path recovery limitation and duplicate tolerance.

## Assumptions

- Ack-after-uploaded-part is the new default.
- Duplicates after crash are acceptable.
- Within-file byte order is not preserved across upload retries: a failed part's bytes are nacked and rejoin the file later, after whatever was processed in the meantime. Bento makes no message-ordering guarantees, so this is acceptable.
- Because failures never abort, a key that permanently fails (or is simply never completed) leaves an incomplete multipart upload on S3. Operators should configure an `AbortIncompleteMultipartUpload` bucket lifecycle rule to reap orphans.
- No bespoke give-up/finalize mechanism is built. Dropping un-writable data so a file can finish is left to standard composition: wrap the output in `fallback` to divert un-writable records to a dead-letter destination. This was chosen over a built-in "finalize the durable prefix + drop the tail" to stay idiomatic (Bento never drops data by default) and because publishing a truncated object by default is undesirable.
- Ack-after-durable requires upstream lookahead: acks are released only as more messages flow (sealing/releasing parts) or on close. The output is therefore **incompatible with `drop_on`** (which blocks for each ack before sending the next message → deadlock) and with inputs limited to a single in-flight message. `fallback` works because it forwards without waiting on acknowledgement.
- Bounded inputs are safe only when they can close their transaction channel without first waiting for the final message acknowledgement. `read_until` with a `check` condition waits for the matching final transaction to be acked before closing, so it can still deadlock with a sub-5 MiB final buffer. Larger streams may make progress because full multipart parts can be uploaded and acknowledged during normal message flow before the final close. Prefer `read_until.idle_timeout` for finite drain-and-exit jobs: once no more messages arrive for the idle period, the input closes normally and `aws_s3_stream` can finalize and ack the buffered tail.
- No cache resource, manifest, or `resume_prefix` is added in this version.
