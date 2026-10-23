/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.connector.operations;

import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.RandomAccessContent;
import org.wso2.org.apache.commons.vfs2.util.RandomAccessMode;
import org.apache.synapse.MessageContext;
import org.apache.synapse.stream.StreamCommit;
import org.apache.synapse.stream.StageArtifact;
import org.apache.synapse.stream.StageDestination;
import org.apache.synapse.stream.ByteSize;
import org.apache.synapse.stream.CheckpointFrequency;
import org.apache.synapse.stream.StreamContext;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamSink;
import org.wso2.carbon.connector.connection.FileSystemHandler;
import org.wso2.carbon.connector.exception.InvalidConfigurationException;
import org.wso2.carbon.connector.utils.Utils;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.org.apache.commons.vfs2.FileObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Tail of a stream sequence: pulls the chain to completion and writes it to a file.
 *
 * <pre>
 *   &lt;file.streamWrite configKey="conn1"&gt;
 *       &lt;path&gt;out/big.csv&lt;/path&gt;
 *   &lt;/file.streamWrite&gt;
 * </pre>
 *
 * <p>This is the only operator in the chain that reads, so the entire transfer happens inside
 * {@link #consume}. The sequence holds this operation's parameters bound for that whole call, unlike a
 * source or transform where they are bound only for the build.
 *
 * <h2>Writes to a temporary name and renames on success</h2>
 * A partial file sitting at the destination is the worst available outcome: it looks complete, so a
 * later run reads it as complete and nothing reports a problem. So the output goes to
 * {@code <path>.part} and is moved into place only when the run succeeded — {@link StreamCommit}'s
 * {@code close()} publishes, {@code abort()} discards, and the sequence calls one or the other
 * depending on the outcome.
 */
public class StreamWrite extends AbstractConnector implements StreamSink {

    /**
     * Keeps a resume position, in bytes, and writes no artifact of its own.
     *
     * <p>Legal because a sink is always last: a position that outlives a failure is only dangerous
     * when something downstream has already been handed output it will not be handed again, and
     * nothing is downstream of a sink.
     *
     * <p>Without this a download could not resume at all. {@code hasDurableState()} is true only when
     * some stage declares {@code checkpointed()} or {@code materialises()}, so a chain of
     * {@code blobStreamRead -> file.streamWrite} had no durable state, was therefore not resumable,
     * and — because only a resumable run stops on shutdown (ADR-0037) — could not be stopped either.
     * Killing a 500 GB download at 450 started it again from zero.
     */
    @Override
    public boolean checkpointed() {
        return true;
    }

    /**
     * Where this sink will resume, in bytes, so the framework can start a seekable source there.
     *
     * <p>Trivial here because the checkpoint is already counted in bytes — a sink that records blocks
     * or records would have to convert. Returns the <b>reconciled</b> position, not the raw
     * checkpoint: see {@link StageArtifact#resumeAgainst}, which is asked not to discard here --
     * this is a question about where the run will start, asked before it has started, and answering
     * it must not cut the part file as a side effect.
     */
    @Override
    public long durableByteOffset(StreamContext ctx) throws IOException {
        try {
            Resolved resolved = resolve(ctx);
            long blockBytes = blockBytes(ctx);
            return ctx.artifact("blocks", FORMAT_VERSION, checkpointFrequency(ctx))
                    .resumeAgainst(new PartFile(resolved.part, blockBytes), false) * blockBytes;
        } catch (StreamException cannotSay) {
            return -1L;
        }
    }


    private static final String NAME = "file.streamWrite";

    private static final int BUFFER_BYTES = 64 * 1024;


    /**
     * Default size of a write block, which is this stage's unit of work.
     *
     * <p>Checkpoint positions count blocks, and {@code checkpointFrequency} counts blocks, so this is
     * what makes that number mean something a deployer chose. 8 MiB suits local disk and a LAN share;
     * a high-latency mount wants more, which is why it is configurable at all.
     */
    private static final long DEFAULT_WRITE_BLOCK_BYTES = 8L * 1024 * 1024;

    /**
     * One block per checkpoint when the deployer says nothing.
     *
     * <p>Coarser than it looks: a block is 8 MiB by default, so this is an fsync per 8 MiB and not
     * per byte. And a reprocessed block here costs only bandwidth — the bytes go into a part file
     * nothing outside the run can see (invariant 5), and resume overwrites the same offsets — so
     * raising it trades re-moved bytes for throughput and never a repeated side effect.
     *
     * <p>{@code writeBlock} sizes the unit and this counts them. Both are parameters of this
     * operation, in the same form, because a deployer sizing the window needs both halves.
     */
    private static final int DEFAULT_CHECKPOINT_FREQUENCY = 1;

    /**
     * Versions the meaning of this stage's checkpoint. The position is a byte count into the part
     * file, which is also the byte count consumed from upstream — this sink changes no volume.
     */
    private static final String FORMAT_VERSION = "file-bytes-1";

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        throw new ConnectException(NAME + " is only valid inside a <streamSequence>. It consumes a lazy"
                + " chain rather than mediating a message, so there is nothing for it to do in a"
                + " sequence. Use file.write instead.");
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void consume(InputStream upstream, StreamContext ctx) throws StreamException, IOException {

        Resolved resolved = resolve(ctx);
        FileObject target = resolved.target;
        FileObject part = resolved.part;
        String path = resolved.path;

        ctx.resources().register(NAME + ":target", target::close);
        ctx.resources().register(NAME + ":part", part::close);

        long blockBytes = blockBytes(ctx);
        StageArtifact checkpoint = ctx.artifact("blocks", FORMAT_VERSION, checkpointFrequency(ctx));
        long resumeFrom = checkpoint.resumeAgainst(new PartFile(part, blockBytes), true) * blockBytes;

        // Skip only what the source did not already skip. Both ends read upstreamStartsAt(), so this
        // is zero when the framework started a seekable source at our resume point, and the whole
        // prefix when it could not — the old behaviour, still correct, just slower.
        long discard = resumeFrom - ctx.upstreamStartsAt();
        if (discard > 0) {
            skip(upstream, discard, path);
            log.info(NAME + " resuming '" + path + "' at byte " + resumeFrom + "; re-read and"
                    + " discarded " + discard + " bytes the source could not skip itself");
        } else if (resumeFrom > 0) {
            log.info(NAME + " resuming '" + path + "' at byte " + resumeFrom
                    + "; the source started there, so nothing is re-read");
        }

        // Append when resuming. The part file already holds every byte below resumeFrom, truncated to
        // exactly that length above.
        OutputStream out = part.getContent().getOutputStream(resumeFrom > 0);

        // Registered as committing, so the run's outcome decides whether this is published. A close
        // failure also fails the transfer: every byte may have been read, but the destination is not in
        // the state the caller was told it would be.
        ctx.resources().registerCommitting(NAME + ":commit", new RenameOnCommit(out, part, target));

        byte[] buffer = new byte[BUFFER_BYTES];
        long written = resumeFrom;
        long sinceProgress = 0L;
        long sinceReport = 0L;
        int read;

        while ((read = upstream.read(buffer)) != -1) {
            out.write(buffer, 0, read);
            written += read;
            sinceProgress += read;
            sinceReport += read;

            if (sinceProgress >= blockBytes) {
                sinceProgress = 0L;
                // Records are not knowable here: a sink sees a byte chain, not a structure.
                ctx.job().progress(ctx.stageName(), written, 0L);
            }

            // One report per completed block. The framework applies checkpointFrequency on
            // top, so a deployer asking for every fourth block gets exactly that.
            if (sinceReport >= blockBytes) {
                sinceReport = 0L;

                // The ordering contract, and the operator's half of it: the bytes are pushed out
                // before the position moves. flush() is not fsync — VFS exposes no sync, and over
                // SFTP or SMB there is nothing local to sync — so a machine crash can still leave the
                // checkpoint ahead of what survived. That is what resumeAgainst reconciles on the
                // next attempt, and it is weaker than the framework's own artifact handling, which
                // does fsync.
                // A process kill, which is what a resume test does, is fully covered: the page cache
                // outlives the process.
                out.flush();
                checkpoint.unitDone(written / blockBytes);
                // Same number the checkpoint took, so a watcher and a resume agree (ADR-0069).
                ctx.status("written " + (written / blockBytes) + " blocks (" + written + " bytes)");
            }

            // Checked every buffer, NOT every checkpoint interval. These were one branch, which tied
            // how often a stop is noticed to how often the position advances -- so a sink that had not
            // yet reached 8 MiB could not be stopped at all. During a graceful shutdown of a transfer
            // over a slow link that is the normal case, not an edge one: the run that exposed this had
            // written nothing, and the stop was seen 96s after it was asked for, at the first interval.
            // Reading a volatile flag per 64 KiB buffer costs nothing worth measuring.
            if (ctx.job().isCancelled()) {
                throw new StreamException("transfer to '" + path + "' was cancelled after "
                        + written + " bytes", false);
            }
        }

        ctx.job().progress(ctx.stageName(), written, 0L);
    }

    /** Where the target and its part file live, resolved once and reused. */
    private static final class Resolved {
        private final FileObject target;
        private final FileObject part;
        private final String path;

        private Resolved(FileObject target, FileObject part, String path) {
            this.target = target;
            this.part = part;
            this.path = path;
        }
    }

    /** The deployer's cadence, in blocks of {@link #blockBytes}. See {@link CheckpointFrequency}. */
    private static int checkpointFrequency(StreamContext ctx) throws StreamException {
        MessageContext msg = ctx.message();
        Object value = msg == null ? null
                : ConnectorUtils.lookupTemplateParamater(msg, CheckpointFrequency.PARAMETER);
        return CheckpointFrequency.of(value == null ? null : value.toString(),
                DEFAULT_CHECKPOINT_FREQUENCY, NAME);
    }

    /**
     * This stage's unit of work, in bytes, from the {@code writeBlock} parameter.
     *
     * <p>Checkpoint positions count these, and so does {@code checkpointFrequency}, so the number a
     * deployer sets is a number of blocks they also chose the size of.
     */
    private static long blockBytes(StreamContext ctx) throws StreamException {
        MessageContext msg = ctx.message();
        Object amount = msg == null ? null : ConnectorUtils.lookupTemplateParamater(msg, "writeBlock");
        Object unit = msg == null ? null : ConnectorUtils.lookupTemplateParamater(msg, "writeBlockUnit");
        Long bytes = ByteSize.bytes(amount == null ? null : amount.toString(),
                unit == null ? null : unit.toString(), NAME, "writeBlock");
        return bytes == null ? DEFAULT_WRITE_BLOCK_BYTES : bytes;
    }

    private Resolved resolve(StreamContext ctx) throws StreamException {
        MessageContext msg = ctx.message();
        if (msg == null) {
            throw new StreamException(NAME + " needs the invoking message to read its parameters from");
        }
        String connection;
        try {
            connection = Utils.getConnectionName(msg);
        } catch (InvalidConfigurationException e) {
            throw new StreamException(NAME + " has no connection configured. Set configKey to a file"
                    + " connection, as on any other file operation.", e);
        }
        Object pathParam = ConnectorUtils.lookupTemplateParamater(msg, "path");
        String path = pathParam == null ? null : pathParam.toString().trim();
        if (path == null || path.isEmpty()) {
            throw new StreamException(NAME + " requires a <path> parameter");
        }
        try {
            FileSystemHandler handler = Utils.getFileSystemHandler(connection);
            String base = handler.getBaseDirectoryPath();
            FileObject target = handler.resolveFileWithSuspension(base + path);
            FileObject part = handler.resolveFileWithSuspension(base + path + ".part");
            FileObject parent = target.getParent();
            if (parent != null && !parent.exists()) {
                parent.createFolder();
            }
            return new Resolved(target, part, path);
        } catch (Exception e) {
            throw new StreamException("cannot open target '" + path + "' on connection '"
                    + connection + "'", e, true);
        }
    }

    /**
     * The part file, as the framework's reconciliation sees it.
     *
     * <p>Two questions and no judgement: how big is it, and cut it to this. Which of the two numbers
     * wins, and whether cutting happens at all, is {@link StageArtifact#resumeAgainst}'s to decide —
     * this sink used to decide it, and so did the Azure one, in two copies of the same argument.
     *
     * <p>Built per run from the resolved part file, never held in a field: one operator instance
     * serves concurrent runs.
     */
    private static final class PartFile implements StageDestination {

        private final FileObject part;
        private final long blockBytes;

        private PartFile(FileObject part, long blockBytes) {
            this.part = part;
            this.blockBytes = blockBytes;
        }

        @Override
        public long durablePosition() throws IOException {
            try {
                // In blocks, because that is the unit the checkpoint counts. A trailing partial block
                // is not durable work: resume rewrites it from the start of the block.
                return part.exists() ? part.getContent().getSize() / blockBytes : 0L;
            } catch (FileSystemException unreadable) {
                // Not zero. Zero means "it holds nothing", which would re-send the whole file; an
                // unreadable part file is a different thing and the caller must hear about it.
                throw new IOException("cannot size the part file to resume against", unreadable);
            }
        }

        @Override
        public void discardAbove(long position) throws IOException {
            long truncateTo = position * blockBytes;
            try (RandomAccessContent raf =
                         part.getContent().getRandomAccessContent(RandomAccessMode.READWRITE)) {
                raf.setLength(truncateTo);
            }
        }
    }

    /** Discards a prefix the source could not skip itself. */
    private static void skip(InputStream upstream, long bytes, String path) throws IOException {
        byte[] sink = new byte[BUFFER_BYTES];
        long done = 0L;
        while (done < bytes) {
            int want = (int) Math.min(sink.length, bytes - done);
            int read = upstream.read(sink, 0, want);
            if (read < 0) {
                throw new IOException(NAME + " resuming '" + path + "' needed to discard " + bytes
                        + " bytes but the source ended after " + done
                        + "; it is not the source the previous attempt read");
            }
            done += read;
        }
    }

    /**
     * Flushes and renames the part file into place on success; deletes it on failure.
     *
     * <p>Holds the output stream rather than registering it separately, because the order matters: the
     * stream must be closed before the rename, and a resource scope unwinds in reverse registration
     * order. Keeping both in one committing resource makes that ordering local instead of implied.
     */
    private static final class RenameOnCommit implements StreamCommit {

        private final OutputStream out;
        private final FileObject part;
        private final FileObject target;

        private RenameOnCommit(OutputStream out, FileObject part, FileObject target) {
            this.out = out;
            this.part = part;
            this.target = target;
        }

        @Override
        public void close() throws IOException {
            out.close();
            if (target.exists()) {
                target.delete();
            }
            part.moveTo(target);
        }

        @Override
        public void abort() {
            // Runs while a failure is already being reported, so nothing here may throw: an exception
            // would compete with the diagnosis the caller actually needs.
            try {
                out.close();
            } catch (Exception ignored) {
                // the write already failed, or the stream is already closed
            }
            try {
                if (part.exists()) {
                    part.delete();
                }
            } catch (Exception ignored) {
                // leaving a .part behind is untidy, not incorrect: its name cannot be mistaken for a
                // finished artifact, which is the property that matters
            }
        }
    }
}
