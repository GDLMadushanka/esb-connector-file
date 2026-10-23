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
import org.apache.synapse.stream.StreamException;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.function.BooleanSupplier;

/**
 * An {@link InputStream} over a remote file that survives a dropped connection.
 *
 * <p>A transfer reads its source through <b>one</b> stream for as long as it takes, and at these sizes
 * that is hours. On SFTP that stream is a single channel over a single TCP connection; on SMB it is a
 * session that an idle timeout or a failover can end. When the connection goes, the next {@code read}
 * throws — "Pipe closed", "Connection reset" — and without this class that ends the transfer, although
 * nothing about the data was wrong and the destination was keeping up.
 *
 * <p>This wrapper counts the bytes it has handed on. When a read fails it closes the broken stream,
 * pauses, opens the source again at exactly the byte it had reached and carries on. Whatever is reading
 * never sees the gap: no byte is repeated and none is lost.
 *
 * <h2>The budget is per attempt, and it is cleared by progress</h2>
 * {@code maxRetries} bounds <b>consecutive</b> failures. A successful read resets the count, so a link
 * that drops once every 50 GB is a transfer that completes, while one that cannot move a single byte
 * fails after the budget rather than retrying for ever. When the budget is spent the original failure is
 * rethrown unchanged, and the framework's own retry and resume take over from there.
 *
 * <h2>A stream that ends early is not the end of the file</h2>
 * The nastier half of a dropped connection is the one that does not throw. A truncated transfer can
 * surface as a clean end-of-file, which looks exactly like a complete short file and produces a
 * destination that is plausible, complete-looking and wrong. So end-of-file before the size the source
 * had when it was opened is treated as a failure and resumed like any other, and if the budget is spent
 * it is reported as an {@link EOFException} — which the framework classifies as <b>not</b> retryable,
 * because reading it again will not make the bytes appear.
 *
 * <h2>What this is not</h2>
 * It is not a checkpoint. It survives a connection, not a crash: everything it knows is in memory and a
 * killed process loses it. Resume across attempts is the framework's, keyed off the sink's durable
 * position. The two compose — this one keeps an attempt alive, that one starts the next attempt in the
 * right place — but neither replaces the other.
 *
 * <p>Not thread-safe, and does not need to be: a sequence is pulled by one thread.
 */
public class ResumableInputStream extends InputStream {

    private static final Log log = LogFactory.getLog(ResumableInputStream.class);

    /** How long a pause may run before cancellation is looked at again. */
    private static final long CANCEL_CHECK_MS = 250L;

    /**
     * Opens the source positioned at an absolute byte offset.
     *
     * <p>Takes the offset rather than the "is this a reopen" flag that would be enough to make the
     * class work, so that a source able to start partway can do so: over SFTP a positioned open is one
     * request, where reaching byte N by skipping re-reads N bytes down the wire. At 500 GB that is the
     * difference between a resume and another transfer. An implementation with no such facility skips,
     * and nothing above here can tell which happened.
     */
    @FunctionalInterface
    public interface Opener {

        /**
         * @param from absolute byte offset the returned stream must begin at
         * @return the stream and what is known about the whole source
         * @throws IOException if the source cannot be reached, or is shorter than {@code from}
         */
        Opened open(long from) throws IOException;
    }

    /** What an {@link Opener} returns: a stream at the byte asked for, and the size of the whole source. */
    public static final class Opened {

        private final InputStream stream;
        private final long size;

        /**
         * @param stream positioned at the requested byte
         * @param size   the size of the whole source, or a negative number when it is not known —
         *               which turns the early-end check off rather than guessing at it
         */
        public Opened(InputStream stream, long size) {
            this.stream = stream;
            this.size = size;
        }

        InputStream stream() {
            return stream;
        }

        long size() {
            return size;
        }
    }

    private final Opener opener;
    private final String what;
    private final long start;
    private final int maxRetries;
    private final long retryDelayMs;
    private final BooleanSupplier cancelled;

    private InputStream current;
    private long served;
    private long total = -1L;
    private boolean opened;
    private int failures;
    private int reopens;

    /**
     * Constructs the stream without opening anything: the first open happens on the first
     * {@code read}, so this is safe to build inside an operator's {@code open()}, where no I/O is
     * allowed.
     *
     * @param opener       opens the source at a byte offset
     * @param what         a name for log lines — the configured path, never a resolved URI, which on
     *                     SFTP and SMB carries the password
     * @param start        the first byte to deliver; the deployer's offset plus any resume offset
     * @param maxRetries   consecutive failures without progress tolerated; 0 turns the behaviour off
     * @param retryDelayMs pause before a retry, multiplied by the failure count
     * @param cancelled    whether the run has been stopped; polled during a pause so a stop is not
     *                     waited out. {@code null} means never cancelled
     */
    public ResumableInputStream(Opener opener, String what, long start, int maxRetries,
                                long retryDelayMs, BooleanSupplier cancelled) {
        this.opener = opener;
        this.what = what;
        this.start = Math.max(0L, start);
        this.maxRetries = Math.max(0, maxRetries);
        this.retryDelayMs = Math.max(0L, retryDelayMs);
        this.cancelled = cancelled == null ? () -> false : cancelled;
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return n < 0 ? -1 : (one[0] & 0xFF);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        while (true) {
            String during = "read";
            try {
                if (current == null) {
                    during = "open";
                    openNow();
                    during = "read";
                }
                int n = current.read(b, off, len);
                if (n > 0) {
                    served += n;
                    // Progress clears the budget: what it bounds is being stuck, not a long transfer
                    // over a link that occasionally drops.
                    failures = 0;
                    return n;
                }
                if (n < 0 && total >= 0 && position() < total) {
                    throw new EOFException("end of stream at byte " + position() + " of " + total
                            + " reading '" + what + "'");
                }
                return n;
            } catch (IOException broken) {
                // Released before the budget is consulted, so the handle goes even on the last failure.
                closeQuietly();
                afterFailure(broken, during);
            }
        }
    }

    /**
     * {@code skip} is deliberately <b>not</b> overridden. {@link InputStream}'s own implementation
     * reads into a scratch buffer, so skipped bytes go through {@link #read(byte[], int, int)} and are
     * counted — delegating to the underlying stream instead would skip bytes this one never saw, and
     * the next reopen would resume at the wrong byte and silently lose them.
     */

    @Override
    public int available() {
        try {
            return current == null ? 0 : current.available();
        } catch (IOException broken) {
            // A hint, not a promise. The next read reports the failure properly and resumes.
            return 0;
        }
    }

    @Override
    public void close() throws IOException {
        InputStream in = current;
        current = null;
        if (in != null) {
            in.close();
        }
    }

    /** How many times the source was opened again after a failure; 0 when nothing went wrong. */
    public int reopens() {
        return reopens;
    }

    /** The absolute offset of the next byte this stream will deliver. */
    public long position() {
        return start + served;
    }

    private void openNow() throws IOException {
        Opened fresh = opener.open(position());
        current = fresh.stream();
        if (!opened) {
            // The size as the FIRST open found it, and never moved afterwards. The early-end check
            // exists to catch a stream that stopped short; taking the size from a later open would let
            // a shrinking source agree with itself.
            total = fresh.size();
            opened = true;
            return;
        }
        reopens++;
        log.info("Reading of '" + what + "' resumed at byte " + position()
                + " after a connection failure (reopen " + reopens + ").");
    }

    /** Spend one unit of the failure budget and pause, or give up by rethrowing the failure. */
    private void afterFailure(IOException cause, String during) throws IOException {
        if (++failures > maxRetries) {
            throw cause;
        }
        log.warn("Reading of '" + what + "' failed during " + during + " at byte " + position()
                + " (failure " + failures + " of " + maxRetries + " tolerated): " + cause
                + ". Opening the source again and resuming.", cause);
        pause(retryDelayMs * failures, cause);
    }

    /**
     * Wait before the next attempt, in slices, so that a stop asked for during the pause is acted on
     * rather than waited out. The pause grows with each failure and the thread sitting in it is the one
     * the sink is blocked on, so a cancelled run would otherwise take the whole budget to stop.
     */
    private void pause(long millis, IOException cause) throws IOException {
        long deadline = System.currentTimeMillis() + millis;
        while (true) {
            if (cancelled.getAsBoolean()) {
                // Not a transient failure, so it is marked non-retryable: a transfer somebody stopped
                // must not be queued again on the strength of the drop that happened to precede it.
                String stopped = "reading of '" + what + "' was cancelled after " + position()
                        + " bytes, while waiting to resume from a connection failure";
                throw new IOException(stopped, new StreamException(stopped, false));
            }
            long left = deadline - System.currentTimeMillis();
            if (left <= 0L) {
                return;
            }
            try {
                Thread.sleep(Math.min(left, CANCEL_CHECK_MS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                InterruptedIOException stop =
                        new InterruptedIOException("interrupted while waiting to resume '" + what + "'");
                stop.initCause(cause);
                throw stop;
            }
        }
    }

    private void closeQuietly() {
        InputStream in = current;
        current = null;
        if (in != null) {
            try {
                in.close();
            } catch (IOException alreadyBroken) {
                log.debug("Could not close the broken stream over '" + what + "'.", alreadyBroken);
            }
        }
    }
}
