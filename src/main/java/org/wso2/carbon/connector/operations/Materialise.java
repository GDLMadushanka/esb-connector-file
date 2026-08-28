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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.stream.StreamCommit;
import org.apache.synapse.stream.StreamContext;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamTransform;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Writes everything passing through it to a durable artifact, and passes it through unchanged.
 *
 * <pre>
 *   &lt;file.materialise name="spill"/&gt;
 * </pre>
 *
 * <p><b>TEMPORARY.</b> This does not belong in the file connector — it touches no file system, no
 * connection and no VFS, so it fails the rule that an operator belongs to the connector owning the
 * resource it works on. It lives here only because standing up a separate connector to prove one
 * capability was not worth it. When a data-processing connector exists, move it and delete this note.
 *
 * <h2>What it is for</h2>
 * Two things. It is the smallest possible exercise of {@code materialises()} — the workspace directory,
 * {@link StreamContext#artifact()}, temp-write-then-rename, discard-on-failure and resume — with no
 * record parsing or sequence invocation to confuse a failure with. And it is genuinely useful in its own
 * right: it is the framework-inserted spill made explicit, so a failure downstream re-derives from this
 * artifact rather than from the original source.
 *
 * <h2>Resume</h2>
 * On a re-run it finds its artifact already present and returns a stream over it, ignoring its upstream
 * entirely. Invariant 2 means that upstream has still been built — which costs nothing, because
 * {@code open()} and {@code wrap()} perform no I/O — so it tells the pipeline through
 * {@link StreamContext#resumedFromArtifact()}, which releases those stages instead of holding a remote
 * connection nothing will read.
 *
 * <h2>Why the write is lazy</h2>
 * Bytes are copied to the artifact as they are pulled, not in a pass of their own. An eagerly
 * materialising operator reads its whole input before emitting anything, so the pipeline pays two full
 * passes over the data and the first byte downstream waits for the last byte upstream.
 */
public class Materialise extends AbstractConnector implements StreamTransform {

    private static final Log log = LogFactory.getLog(Materialise.class);

    private static final String NAME = "file.materialise";

    /** Suffix of the file being written. A partial artifact must never be visible under its final name. */
    private static final String PARTIAL = ".part";

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        throw new ConnectException(NAME + " is only valid inside a <streamPipeline>. It contributes a"
                + " link to a lazy chain rather than mediating a message, so there is nothing for it to"
                + " do in a sequence.");
    }

    @Override
    public String name() {
        return NAME;
    }

    /**
     * Writes a durable artifact, so this stage ends a segment and gets a workspace directory of its own.
     * The pipeline requires an explicit {@code name} on any operator declaring this, because the
     * directory is keyed by it and this class is shared by every stage that uses the operation.
     */
    @Override
    public boolean materialises() {
        return true;
    }

    /**
     * A byte-for-byte copy of its input, so a downstream stage may skip forward through it. Not a claim
     * about the source: whether the same bytes arrive again is the source's business.
     */
    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public InputStream wrap(InputStream upstream, StreamContext ctx) throws StreamException {
        Path artifact = ctx.artifact();

        if (Files.exists(artifact)) {
            // Present under its final name, so a previous run finished writing it — that is what
            // temp-write-then-rename buys. Everything above this stage is now orphaned.
            ctx.resumedFromArtifact();
            log.info(NAME + " stage '" + ctx.stageName() + "' found its artifact at " + artifact
                    + "; resuming from it and ignoring upstream");
            try {
                return ctx.resources().register(ctx.stageName() + ":artifact",
                        Files.newInputStream(artifact));
            } catch (IOException e) {
                throw new StreamException("cannot read the existing artifact at " + artifact, e, true);
            }
        }

        // No I/O yet: opening the partial file is deferred to the first read, like any other operator.
        return new TeeStream(upstream, ctx, artifact);
    }

    /**
     * Copies what it serves into the artifact, opening on the first read.
     *
     * <p>The output is registered as a committing resource, so the run's outcome decides whether the
     * partial file is renamed into place or deleted. A failed run must leave nothing under the final
     * name, or the next attempt reads it as a finished segment.
     */
    private static final class TeeStream extends FilterInputStream {

        private final StreamContext ctx;
        private final Path artifact;
        private final Path partial;
        private OutputStream out;

        /**
         * Whether upstream reached end-of-stream. The artifact is only complete if it did, and nothing
         * else can tell: this stage writes as a side effect of being read, so a downstream sink that
         * stops early leaves a short file with no other symptom.
         */
        private boolean drained;

        private TeeStream(InputStream upstream, StreamContext ctx, Path artifact) {
            super(upstream);
            this.ctx = ctx;
            this.artifact = artifact;
            this.partial = artifact.resolveSibling(artifact.getFileName() + PARTIAL);
        }

        private OutputStream out() throws IOException {
            if (out == null) {
                out = Files.newOutputStream(partial);
                ctx.resources().registerCommitting(ctx.stageName() + ":artifact",
                        new RenameOnCommit(out, partial, artifact, this));
            }
            return out;
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b == -1) {
                drained = true;
            } else {
                out().write(b);
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int read = in.read(b, off, len);
            if (read == -1) {
                drained = true;
            } else if (read > 0) {
                out().write(b, off, read);
            }
            return read;
        }

        boolean isDrained() {
            return drained;
        }

        /**
         * Does not close its upstream or its output. Both belong to the run's ResourceScope, which
         * unwinds them in reverse registration order — and the output must be committed or aborted by
         * the scope rather than closed here, since only the scope knows whether the run succeeded.
         */
        @Override
        public void close() {
            // intentionally empty; see javadoc
        }
    }

    /**
     * Publishes the artifact on success, discards it on failure — and treats "the run succeeded but
     * upstream was never drained" as a failure for this artifact.
     *
     * <p>That last case is the one worth explaining. A sink is obliged to read to end-of-stream, but
     * nothing enforces it, and this stage writes only as a side effect of being read. So a sink that
     * returns early leaves a short file and a successful run — and renaming that into place would
     * publish a truncated artifact under the name that means complete, which the next run would resume
     * from and trust. Losing the artifact is recoverable; trusting a short one is not.
     */
    private static final class RenameOnCommit implements StreamCommit {

        private final OutputStream out;
        private final Path partial;
        private final Path artifact;
        private final TeeStream source;

        private RenameOnCommit(OutputStream out, Path partial, Path artifact, TeeStream source) {
            this.out = out;
            this.partial = partial;
            this.artifact = artifact;
            this.source = source;
        }

        @Override
        public void close() throws IOException {
            out.close();
            if (!source.isDrained()) {
                Files.deleteIfExists(partial);
                throw new IOException("the chain above '" + artifact.getParent().getFileName()
                        + "' was not read to end-of-stream, so its artifact is incomplete and has been"
                        + " discarded. A sink must drain what it is given or throw; returning early"
                        + " reports success on a partial transfer");
            }
            Files.move(partial, artifact, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
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
                Files.deleteIfExists(partial);
            } catch (Exception ignored) {
                // leaving a .part behind is untidy, not incorrect: its name cannot be mistaken for a
                // finished artifact, which is the property that matters
            }
        }
    }
}
