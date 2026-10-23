/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.connector.operations;

import org.apache.synapse.stream.StreamException;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * What a dropped connection must and must not do to a transfer.
 *
 * <p>Driven entirely through a fake {@link ResumableInputStream.Opener}: no VFS, no network, no
 * sleeping. The two properties worth the most here are that the opener is asked for the
 * <b>absolute</b> byte the read had reached — get that wrong and a resumed transfer is silently short
 * — and that an early end-of-file is a failure rather than the end of the file.
 */
public class ResumableInputStreamTest {

    private static final byte[] DATA = content(1000);

    // ----- the fake source -------------------------------------------------------------------

    /**
     * A source of {@code all} that can be told to break each of its streams after a given number of
     * bytes, and that records every offset it was asked to open at.
     */
    private static final class Source implements ResumableInputStream.Opener {

        private final byte[] all;
        private final boolean silentEof;
        private final List<Long> breaks = new ArrayList<>();
        private final List<Long> asked = new ArrayList<>();
        private final List<Integer> failingOpens = new ArrayList<>();

        private long reportedSize;
        private long shrinksTo = -1L;
        private int opens;

        /** @param breaks bytes each successive stream serves before it breaks; -1 for an intact one */
        Source(byte[] all, boolean silentEof, long... breaks) {
            this.all = all;
            this.silentEof = silentEof;
            this.reportedSize = all.length;
            for (long each : breaks) {
                this.breaks.add(each);
            }
        }

        @Override
        public ResumableInputStream.Opened open(long from) throws IOException {
            int index = opens++;
            asked.add(from);
            if (failingOpens.contains(index)) {
                throw new IOException("Connection refused");
            }
            long length = (shrinksTo >= 0 && index > 0) ? shrinksTo : all.length;
            if (from > length) {
                throw new EOFException("'f' is only " + length + " bytes long; it cannot be read from"
                        + " byte " + from);
            }
            long breakAfter = breaks.isEmpty() ? -1L : breaks.remove(0);
            return new ResumableInputStream.Opened(
                    new Breaking(all, (int) from, (int) length, breakAfter, silentEof), reportedSize);
        }
    }

    /** One stream from the fake source: serves bytes until it is told to stop, then breaks. */
    private static final class Breaking extends InputStream {

        private final byte[] all;
        private final int limit;
        private final long breakAfter;
        private final boolean silentEof;
        private int pos;
        private long served;

        Breaking(byte[] all, int from, int limit, long breakAfter, boolean silentEof) {
            this.all = all;
            this.limit = limit;
            this.pos = from;
            this.breakAfter = breakAfter;
            this.silentEof = silentEof;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (breakAfter >= 0 && served >= breakAfter) {
                if (silentEof) {
                    return -1;                       // the drop that does not announce itself
                }
                throw new IOException("Pipe closed");
            }
            if (pos >= limit) {
                return -1;
            }
            int want = len;
            if (breakAfter >= 0) {
                want = (int) Math.min(want, breakAfter - served);
            }
            int n = Math.min(want, limit - pos);
            System.arraycopy(all, pos, b, off, n);
            pos += n;
            served += n;
            return n;
        }
    }

    // ----- nothing is wrong ------------------------------------------------------------------

    @Test
    public void anIntactStreamIsPassedStraightThrough() throws IOException {
        Source source = new Source(DATA, false);
        ResumableInputStream in = resumable(source, 0L, 3, 0L, false);

        assertEquals(drain(in), DATA);
        assertEquals(in.reopens(), 0);
        assertEquals(source.asked, List.of(0L), "an intact read opens the source exactly once");
    }

    // ----- a drop mid-file -------------------------------------------------------------------

    @Test
    public void aDropMidFileResumesAtTheSameByte() throws IOException {
        Source source = new Source(DATA, false, 400L);
        ResumableInputStream in = resumable(source, 0L, 3, 0L, false);

        assertEquals(drain(in), DATA, "no byte repeated and none lost");
        assertEquals(in.reopens(), 1);
        assertEquals(source.asked, List.of(0L, 400L));
    }

    @Test
    public void progressClearsTheBudgetSoAFlakyLinkStillCompletes() throws IOException {
        // Three drops, but each one after bytes moved, against a budget of one consecutive failure.
        Source source = new Source(DATA, false, 100L, 250L, 600L);
        ResumableInputStream in = resumable(source, 0L, 1, 0L, false);

        assertEquals(drain(in), DATA);
        assertEquals(in.reopens(), 3);
        assertEquals(source.asked, List.of(0L, 100L, 350L, 950L));
    }

    @Test
    public void aFailureOpeningAgainSpendsTheBudgetToo() throws IOException {
        Source source = new Source(DATA, false, 400L);
        source.failingOpens.add(1);                  // the reopen itself cannot connect
        ResumableInputStream in = resumable(source, 0L, 2, 0L, false);

        assertEquals(drain(in), DATA);
        assertEquals(in.reopens(), 1, "the open that failed is not a reopen; the one after it is");
        assertEquals(source.asked, List.of(0L, 400L, 400L));
    }

    // ----- the budget runs out ---------------------------------------------------------------

    @Test
    public void whenTheBudgetIsSpentTheOriginalFailureIsThrown() {
        Source source = new Source(DATA, false, 400L, 0L, 0L);
        ResumableInputStream in = resumable(source, 0L, 2, 0L, false);

        try {
            drain(in);
            fail("a source that stops moving must fail the transfer");
        } catch (IOException stopped) {
            assertEquals(stopped.getMessage(), "Pipe closed",
                    "the failure reported is the one that happened, not a wrapper about retries");
        }
    }

    @Test
    public void zeroRetriesMeansTheFirstFailureIsTheLast() {
        Source source = new Source(DATA, false, 400L);
        ResumableInputStream in = resumable(source, 0L, 0, 0L, false);

        try {
            drain(in);
            fail("retries are off, so the drop must end the transfer");
        } catch (IOException stopped) {
            assertEquals(stopped.getMessage(), "Pipe closed");
            assertEquals(source.asked, List.of(0L), "nothing was opened a second time");
        }
    }

    // ----- the drop that does not throw ------------------------------------------------------

    @Test
    public void aStreamEndingEarlyIsATruncatedReadNotTheEndOfTheFile() throws IOException {
        Source source = new Source(DATA, true, 400L);
        ResumableInputStream in = resumable(source, 0L, 3, 0L, false);

        assertEquals(drain(in), DATA, "the silent half of a dropped connection is still a drop");
        assertEquals(in.reopens(), 1);
        assertEquals(source.asked, List.of(0L, 400L));
    }

    @Test
    public void anEarlyEndThatCannotBeResumedIsReportedAsOne() {
        Source source = new Source(DATA, true, 400L, 0L, 0L);
        ResumableInputStream in = resumable(source, 0L, 1, 0L, false);

        try {
            drain(in);
            fail("a short read must fail rather than pass as a complete short file");
        } catch (IOException stopped) {
            assertTrue(stopped instanceof EOFException,
                    "EOFException is what the framework reads as 'another attempt will not help';"
                            + " got " + stopped.getClass().getName());
            assertTrue(stopped.getMessage().contains("of 1000"), stopped.getMessage());
        }
    }

    @Test
    public void anUnknownSizeTurnsTheEarlyEndCheckOffRatherThanGuessing() throws IOException {
        Source source = new Source(DATA, true, 400L);
        source.reportedSize = -1L;
        ResumableInputStream in = resumable(source, 0L, 3, 0L, false);

        assertEquals(drain(in), Arrays.copyOf(DATA, 400),
                "with no size to compare against, end of stream has to be taken at its word");
        assertEquals(in.reopens(), 0);
    }

    @Test
    public void aSourceThatShrankWhileWeWereAwayIsNotGuessedAt() {
        Source source = new Source(DATA, false, 400L);
        source.shrinksTo = 300L;
        ResumableInputStream in = resumable(source, 0L, 3, 0L, false);

        try {
            drain(in);
            fail("a file now shorter than what was already read cannot be resumed");
        } catch (IOException stopped) {
            assertTrue(stopped instanceof EOFException, stopped.getClass().getName());
        }
    }

    // ----- offsets -----------------------------------------------------------------------------

    @Test
    public void theOpenerIsAskedForAbsoluteBytesNotBytesSinceTheStart() throws IOException {
        Source source = new Source(DATA, false, 250L);
        ResumableInputStream in = resumable(source, 100L, 3, 0L, false);

        assertEquals(drain(in), Arrays.copyOfRange(DATA, 100, 1000));
        assertEquals(source.asked, List.of(100L, 350L),
                "350 is where the read had got to in the file, not how much it had delivered");
    }

    @Test
    public void skippedBytesCountTowardsWhereAResumeBegins() throws IOException {
        Source source = new Source(DATA, false, 500L);
        ResumableInputStream in = resumable(source, 0L, 3, 0L, false);

        assertEquals(in.skip(200L), 200L);
        assertEquals(in.position(), 200L, "skip must go through read(), or the resume point drifts");
        assertEquals(drain(in), Arrays.copyOfRange(DATA, 200, 1000));
        assertEquals(source.asked, List.of(0L, 500L));
    }

    // ----- cancellation -------------------------------------------------------------------------

    @Test
    public void aCancelledRunStopsInsteadOfWaitingOutThePause() {
        Source source = new Source(DATA, false, 400L);
        ResumableInputStream in = resumable(source, 0L, 3, 60_000L, true);

        long began = System.currentTimeMillis();
        try {
            drain(in);
            fail("a cancelled run must not keep reading");
        } catch (IOException stopped) {
            long waited = System.currentTimeMillis() - began;
            assertTrue(waited < 10_000L, "stopped after " + waited + "ms of a 60s pause");
            assertTrue(stopped.getCause() instanceof StreamException,
                    "the framework reads retryability off the cause");
            assertFalse(((StreamException) stopped.getCause()).isRetryable(),
                    "a transfer somebody stopped must not be queued again");
        }
    }

    // ----- helpers ------------------------------------------------------------------------------

    private static ResumableInputStream resumable(Source source, long start, int retries,
                                                  long delayMs, boolean cancelled) {
        return new ResumableInputStream(source, "f", start, retries, delayMs, () -> cancelled);
    }

    private static byte[] content(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (i % 251);
        }
        return bytes;
    }

    /** Reads to the end through an awkward buffer size, so no boundary lines up with a break. */
    private static byte[] drain(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[7];
        int n;
        while ((n = in.read(buffer)) >= 0) {
            out.write(buffer, 0, n);
        }
        return out.toByteArray();
    }
}
