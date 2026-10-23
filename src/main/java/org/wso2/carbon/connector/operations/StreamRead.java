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
import org.apache.synapse.stream.StreamContext;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.SourceIdentity;
import org.apache.synapse.stream.StreamOrigin;
import org.apache.synapse.stream.StreamSource;
import org.wso2.carbon.connector.connection.FileSystemHandler;
import org.wso2.carbon.connector.exception.InvalidConfigurationException;
import org.wso2.carbon.connector.utils.Utils;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.RandomAccessContent;
import org.wso2.org.apache.commons.vfs2.util.RandomAccessMode;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Opens a file as the head of a stream sequence.
 *
 * <pre>
 *   &lt;file.streamRead configKey="conn1"&gt;
 *       &lt;path&gt;in/big.csv&lt;/path&gt;
 *       &lt;offset&gt;0&lt;/offset&gt;
 *   &lt;/file.streamRead&gt;
 * </pre>
 *
 * <p>Unlike {@code file.read} this puts nothing on the message. It contributes the first link of a lazy
 * chain, and bytes travel from here through every operator in one pass, so memory stays bounded by the
 * layer buffers rather than by the size of the file.
 *
 * <h2>An ordinary connector operation that also happens to be a stream operator</h2>
 * There is nothing special about how this is packaged: a template in {@code functions/}, a
 * {@code <class>} mediator, parameters read off the message. The only difference from
 * {@code file.read} is that it implements {@link StreamSource} as well as being a mediator, which is
 * what lets a {@code <streamSequence>} drive it directly instead of mediating it.
 *
 * <h2>Why no configuration is held in fields</h2>
 * One template instance is shared by every sequence and every concurrent run that uses this operation,
 * so anything stored in a field would be overwritten by whichever transfer configured itself last.
 * Parameters are read from the message on each call and captured into locals instead.
 *
 * <p>They are also only bound <b>for the duration of {@link #open}</b> — the sequence pushes the
 * template's context before the call and pops it after. So everything needed later must be captured
 * here; the stream returned is read long afterwards, when nothing is bound.
 *
 * <h2>No I/O in open()</h2>
 * The sequence builds every operator before a byte moves, and a resumed run discards the chain belonging
 * to segments it is skipping — so an eager connect would hold a remote handle, unused, for the whole
 * run. The connection lookup, the resolve, the existence check and the stat are all deferred to the
 * first {@code read()}.
 *
 * <h2>A dropped connection does not end the transfer</h2>
 * The stream handed back is wrapped in a {@link ResumableInputStream}: when the connection goes, the
 * file is opened again at the byte the read had reached and the transfer continues. See
 * {@code readRetries} and {@code readRetryDelay}, and {@link FileOpener} for how a resume reaches byte
 * N — by seeking where the provider allows it, and by skipping where it does not.
 */
public class StreamRead extends AbstractConnector implements StreamSource {

    private static final String NAME = "file.streamRead";

    /**
     * Consecutive read failures without progress tolerated before the transfer fails.
     *
     * <p>On by default rather than opt-in: a transfer long enough to be worth streaming is long enough
     * to meet a connection drop, and failing the whole run over one is the expensive answer — the
     * framework's own retry would re-read everything already sent.
     */
    private static final long DEFAULT_READ_RETRIES = 3L;

    /** Pause before a retry, multiplied by the failure count — so 5s, then 10s, then 15s. */
    private static final long DEFAULT_READ_RETRY_DELAY_MS = 5000L;

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        throw new ConnectException(NAME + " is only valid inside a <streamSequence>. It contributes a"
                + " link to a lazy chain rather than mediating a message, so there is nothing for it to"
                + " do in a sequence. Use file.read instead.");
    }

    @Override
    public String name() {
        return NAME;
    }

    /**
     * Reading a file is a pure function of that file, so the same bytes come back — which is what lets a
     * downstream operator skip forward through this stage. The honest limit: this holds because the
     * source-identity guard verifies the file has not changed underneath a resumed run.
     */
    @Override
    public boolean deterministic() {
        return true;
    }

    /** A file can be opened again by a later attempt, which is what makes a run over it resumable. */
    @Override
    public StreamOrigin origin() {
        return StreamOrigin.REOPENABLE;
    }

    /**
     * Yes — the first byte delivered is the byte at {@link StreamContext#upstreamStartsAt()}, always.
     *
     * <p>What the promise is <b>not</b> is a promise that getting there is cheap. SFTP and local files
     * expose random access, and a resume at byte 450,000,000 is then a single positioned open that
     * never transfers the prefix. A provider without random access is skipped to instead, and what
     * that costs is its own business: SMB2 skips by moving a read offset and arrives as cheaply as a
     * seek, while another provider may read and discard the whole prefix. Either way the offset is
     * honoured, and no route is worse than not declaring the flag — the sink would otherwise have
     * read and discarded the same bytes one stage further along.
     *
     * <p>Declaring it is still worth it for the half where it is cheap, and the contract it has to keep
     * is the one that matters: deliver from that byte, or fail. {@link FileOpener#skipTo} throws rather
     * than returning short, because a source that quietly starts in the wrong place produces a
     * destination missing a slice from its middle with nothing anywhere to notice.
     */
    @Override
    public boolean seekable() {
        return true;
    }

    @Override
    public InputStream open(StreamContext ctx) throws StreamException {

        MessageContext msg = ctx.message();
        if (msg == null) {
            throw new StreamException(NAME + " needs the invoking message to read its parameters from");
        }

        // Captured now, while the template's parameters are still bound. Tenant-qualified here too: the
        // tenant is a property of the message, and one deployed sequence serves every tenant.
        String connection;
        try {
            connection = Utils.getConnectionName(msg);
        } catch (InvalidConfigurationException e) {
            throw new StreamException(NAME + " has no connection configured. Set configKey to a file"
                    + " connection, as on any other file operation.", e);
        }
        String path = param(msg, "path");
        if (path == null || path.trim().isEmpty()) {
            throw new StreamException(NAME + " requires a <path> parameter");
        }
        long offset = longParam(msg, "offset", 0L);
        long retries = longParam(msg, "readRetries", DEFAULT_READ_RETRIES);
        long retryDelay = longParam(msg, "readRetryDelay", DEFAULT_READ_RETRY_DELAY_MS);

        return new LazyFileStream(ctx, connection, path.trim(), offset,
                (int) Math.min(retries, Integer.MAX_VALUE), retryDelay);
    }

    /**
     * One stat, and the identity built from what it returns.
     *
     * <p>Unlike {@link #open}, I/O is expected here: this is how a queued transfer -- which carries no
     * seed -- learns what it is about to read, early enough for the framework to notice that a previous
     * attempt read something else.
     *
     * <p><b>The identity is the connection name, the configured path, the size and the modification
     * time -- never a resolved URI.</b> {@code constructVfsUrl} embeds {@code username:password@} in
     * the base path, and this value is written into a job row on a shared mount and printed whenever a
     * guard mismatch is logged. Size and mtime are folded in deliberately: a file replaced between two
     * attempts is a different source even at the same path, and the framework compares identities for
     * equality only, so anything it should notice has to be inside the string.
     *
     * @return what this source would open, or {@code null} if the message is not available
     * @throws StreamException retryable if the file cannot be reached or is not there yet -- for a
     *                         queued transfer it may well arrive before the next attempt
     */
    @Override
    public SourceIdentity identify(StreamContext ctx) throws StreamException {

        MessageContext msg = ctx.message();
        if (msg == null) {
            return null;
        }
        String connection;
        try {
            connection = Utils.getConnectionName(msg);
        } catch (InvalidConfigurationException e) {
            // open() reports this properly a moment later, naming the stage; a probe that threw here
            // would pre-empt it with a worse message.
            return null;
        }
        String path = param(msg, "path");
        if (path == null || path.trim().isEmpty()) {
            return null;
        }
        String target = path.trim();

        FileObject file = null;
        try {
            FileSystemHandler handler = Utils.getFileSystemHandler(connection);
            file = handler.resolveFileWithSuspension(handler.getBaseDirectoryPath() + target);
            if (!file.exists()) {
                throw new StreamException(NAME + " cannot find '" + target + "' on connection '"
                        + connection + "'", true);
            }
            long size = file.getContent().getSize();
            long lastModified = file.getContent().getLastModifiedTime();
            return new SourceIdentity(connection + "|" + target + "|" + size + "|" + lastModified,
                    size, lastModified);
        } catch (StreamException e) {
            throw e;
        } catch (Exception e) {
            throw new StreamException(NAME + " could not identify '" + target + "' on connection '"
                    + connection + "'", e, true);
        } finally {
            // Closed here, not registered with the scope: this file handle belongs to the probe, and
            // the run will resolve its own when it opens the stream.
            if (file != null) {
                try {
                    file.close();
                } catch (Exception ignore) {
                    // A handle that will not close is not a reason to fail a transfer that has not
                    // started; the connection pool reclaims it.
                }
            }
        }
    }

    private static String param(MessageContext msg, String name) {
        Object value = ConnectorUtils.lookupTemplateParamater(msg, name);
        return value == null ? null : value.toString();
    }

    private static long longParam(MessageContext msg, String name, long fallback)
            throws StreamException {
        String value = param(msg, name);
        if (value == null || value.trim().isEmpty()) {
            return fallback;
        }
        try {
            long parsed = Long.parseLong(value.trim());
            if (parsed < 0L) {
                throw new StreamException(NAME + " has <" + name + ">" + value + "</" + name
                        + ">; it cannot be negative");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new StreamException(NAME + " has <" + name + ">" + value + "</" + name
                    + ">; expected a whole number", e);
        }
    }

    /**
     * Opens on the first read, and not before.
     *
     * <p>Also where source identity is reported, for the same reason: it needs a stat, and a stat is
     * I/O. A run that never pulls this stage therefore reports no source, which is correct — it did not
     * read one.
     */
    private static final class LazyFileStream extends InputStream {

        private final StreamContext ctx;
        private final String connection;
        private final String path;
        private final long offset;
        private final int retries;
        private final long retryDelayMs;
        private InputStream delegate;

        private LazyFileStream(StreamContext ctx, String connection, String path, long offset,
                               int retries, long retryDelayMs) {
            this.ctx = ctx;
            this.connection = connection;
            this.path = path;
            this.offset = offset;
            this.retries = retries;
            this.retryDelayMs = retryDelayMs;
        }

        private InputStream delegate() throws IOException {
            if (delegate != null) {
                return delegate;
            }

            // The deployer's offset is where this source's output begins; the framework's resume offset
            // is a position within that output. They add, and the result is a position in the file.
            long resumeFrom = ctx.upstreamStartsAt();
            long start = offset + resumeFrom;

            // Both registered before anything is opened, and in this order: the scope unwinds
            // last-registered-first, so the stream closes before the handle it came from.
            FileOpener opener = ctx.resources().register(NAME + ":file",
                    new FileOpener(ctx, connection, path));
            ResumableInputStream resumable = new ResumableInputStream(opener, path, start, retries,
                    retryDelayMs, () -> ctx.job().isCancelled());
            delegate = ctx.resources().register(NAME + ":stream", resumable);
            return delegate;
        }

        @Override
        public int read() throws IOException {
            return delegate().read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return delegate().read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return delegate().skip(n);
        }

        @Override
        public int available() throws IOException {
            return delegate().available();
        }

        /**
         * Does not close the delegate. Both the file and its stream are registered with the run's
         * resource scope, which unwinds them last-in-first-out — closing here as well would close them
         * twice, and the second close of a VFS stream is not always harmless.
         */
        @Override
        public void close() {
            // intentionally empty; see javadoc
        }
    }

    /**
     * Resolves the file and opens it at a byte offset, again whenever the connection drops.
     *
     * <p>Owns the VFS handle for the run. Each open releases the previous one — a reopen that kept it
     * would hand the broken connection straight back — and the last is released when the run's resource
     * scope unwinds.
     *
     * <h2>Seek where the provider can, skip where it cannot</h2>
     * SFTP and local files expose {@link RandomAccessContent}, and its {@code seek} turns a start at
     * byte N into a single positioned open: over SFTP that is {@code ChannelSftp.get(path, null, N)},
     * and the bytes below N never cross the network. SMB2 exposes no random access at all, so it is
     * skipped to instead.
     *
     * <p><b>Skipping is not automatically the expensive one.</b> VFS wraps a provider's stream in a
     * {@code BufferedInputStream}, whose {@code skip} delegates downwards, and smbj's own skip moves a
     * read offset without transferring anything — so SMB2 arrives about as cheaply as a seek, measured
     * at 2 ms for 10 MB. A provider whose stream cannot do better will read and discard instead.
     *
     * <p>Which route was taken and <b>how long it took</b> are both logged whenever the start is not
     * zero, because only the second one is a fact about this connection. A resume that is slow should
     * be able to say so itself rather than be predicted here.
     */
    private static final class FileOpener implements ResumableInputStream.Opener, Closeable {

        private static final Log log = LogFactory.getLog(FileOpener.class);

        private final StreamContext ctx;
        private final String connection;
        private final String path;

        private FileObject file;
        private RandomAccessContent random;
        private boolean reported;

        private FileOpener(StreamContext ctx, String connection, String path) {
            this.ctx = ctx;
            this.connection = connection;
            this.path = path;
        }

        @Override
        public ResumableInputStream.Opened open(long from) throws IOException {

            // Whatever a broken connection left behind, before asking for another one.
            releaseQuietly();

            FileObject opening;
            try {
                FileSystemHandler handler = Utils.getFileSystemHandler(connection);
                opening = handler.resolveFileWithSuspension(handler.getBaseDirectoryPath() + path);
                if (!opening.exists()) {
                    throw new IOException("file not found on connection '" + connection + "': " + path);
                }
            } catch (IOException notThere) {
                throw notThere;
            } catch (Exception notReachable) {
                throw new IOException("cannot open '" + path + "' on connection '" + connection + "'",
                        notReachable);
            }
            file = opening;

            long size = opening.getContent().getSize();
            if (!reported) {
                // Once per run, from the first open: this is the size the transfer is about, and a
                // later open must not restate it.
                ctx.job().sourceOpened(size, opening.getContent().getLastModifiedTime());
                reported = true;
            }

            long began = System.currentTimeMillis();
            InputStream in = from > 0L ? seekTo(opening, from) : null;
            boolean seeked = in != null;
            if (in == null) {
                in = opening.getContent().getInputStream();
                if (from > 0L) {
                    skipTo(in, from);
                }
            }
            if (from > 0L) {
                // Timed rather than asserted. Whether getting here was free is a property of the
                // provider and of its stream, and the two do not always agree: SMB2 offers no random
                // access at all, yet its stream skips by moving a read offset, so it arrives as
                // cheaply as a seek. A number settles that for whichever provider is in front of us;
                // a sentence about it would eventually be wrong.
                log.info(NAME + " opened '" + path + "' at byte " + from + " of " + size + " by "
                        + (seeked ? "seeking" : "skipping") + ", in "
                        + (System.currentTimeMillis() - began) + " ms");
            }
            return new ResumableInputStream.Opened(in, size);
        }

        /**
         * A stream positioned at {@code from} without reading what comes before it, or {@code null}
         * when this provider has no random access to offer.
         *
         * <p>Returning {@code null} rather than throwing is deliberate: there is a correct fallback,
         * and a provider that cannot seek is a fact about the provider, not a failure of the transfer.
         * A connection that is genuinely down fails the skip a moment later and reaches the retry
         * budget the ordinary way.
         */
        private InputStream seekTo(FileObject opening, long from) {
            RandomAccessContent content = null;
            try {
                content = opening.getContent().getRandomAccessContent(RandomAccessMode.READ);
                content.seek(from);
                InputStream in = content.getInputStream();
                random = content;
                return in;
            } catch (Exception cannotSeek) {
                if (content != null) {
                    try {
                        content.close();
                    } catch (Exception ignore) {
                        // nothing was promised; the handle below it is released either way
                    }
                }
                log.debug("Random access to '" + path + "' on connection '" + connection + "' is not"
                        + " available; reaching byte " + from + " by reading instead.", cannotSeek);
                return null;
            }
        }

        /**
         * Reads and discards until {@code from}.
         *
         * <p>Throws rather than returning short. A source that quietly begins in the wrong place is the
         * failure {@link StreamRead#seekable()} warns about: the sink skips nothing because the source promised
         * to have skipped, and the destination ends up missing a slice from its middle.
         */
        private void skipTo(InputStream in, long from) throws IOException {
            long remaining = from;
            while (remaining > 0L) {
                long moved = in.skip(remaining);
                if (moved <= 0L) {
                    if (in.read() < 0) {
                        throw new EOFException("'" + path + "' is only " + (from - remaining)
                                + " bytes long; it cannot be read from byte " + from);
                    }
                    moved = 1L;
                }
                remaining -= moved;
            }
        }

        @Override
        public void close() throws IOException {
            RandomAccessContent content = random;
            FileObject open = file;
            random = null;
            file = null;
            try {
                if (content != null) {
                    content.close();
                }
            } finally {
                if (open != null) {
                    open.close();
                }
            }
        }

        /** Release before a reopen: a handle that will not close must not stop the transfer resuming. */
        private void releaseQuietly() {
            try {
                close();
            } catch (Exception alreadyBroken) {
                log.debug("Could not release the previous handle on '" + path + "'.", alreadyBroken);
            }
        }
    }
}
