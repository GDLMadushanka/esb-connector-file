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

import org.apache.synapse.MessageContext;
import org.apache.synapse.stream.StreamContext;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamOrigin;
import org.apache.synapse.stream.StreamSource;
import org.wso2.carbon.connector.connection.FileSystemHandler;
import org.wso2.carbon.connector.exception.InvalidConfigurationException;
import org.wso2.carbon.connector.utils.Utils;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.org.apache.commons.vfs2.FileObject;

import java.io.IOException;
import java.io.InputStream;

/**
 * Opens a file as the head of a stream pipeline.
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
 * what lets a {@code <streamPipeline>} drive it directly instead of mediating it.
 *
 * <h2>Why no configuration is held in fields</h2>
 * One template instance is shared by every pipeline and every concurrent run that uses this operation,
 * so anything stored in a field would be overwritten by whichever transfer configured itself last.
 * Parameters are read from the message on each call and captured into locals instead.
 *
 * <p>They are also only bound <b>for the duration of {@link #open}</b> — the pipeline pushes the
 * template's context before the call and pops it after. So everything needed later must be captured
 * here; the stream returned is read long afterwards, when nothing is bound.
 *
 * <h2>No I/O in open()</h2>
 * The pipeline builds every operator before a byte moves, and a resumed run discards the chain belonging
 * to segments it is skipping — so an eager connect would hold a remote handle, unused, for the whole
 * run. The connection lookup, the resolve, the existence check and the stat are all deferred to the
 * first {@code read()}.
 */
public class StreamRead extends AbstractConnector implements StreamSource {

    private static final String NAME = "file.streamRead";

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        throw new ConnectException(NAME + " is only valid inside a <streamPipeline>. It contributes a"
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

    @Override
    public InputStream open(StreamContext ctx) throws StreamException {

        MessageContext msg = ctx.message();
        if (msg == null) {
            throw new StreamException(NAME + " needs the invoking message to read its parameters from");
        }

        // Captured now, while the template's parameters are still bound. Tenant-qualified here too: the
        // tenant is a property of the message, and one deployed pipeline serves every tenant.
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
        long offset = longParam(msg, "offset");

        return new LazyFileStream(ctx, connection, path.trim(), offset);
    }

    private static String param(MessageContext msg, String name) {
        Object value = ConnectorUtils.lookupTemplateParamater(msg, name);
        return value == null ? null : value.toString();
    }

    private static long longParam(MessageContext msg, String name) throws StreamException {
        String value = param(msg, name);
        if (value == null || value.trim().isEmpty()) {
            return 0L;
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
        private InputStream delegate;

        private LazyFileStream(StreamContext ctx, String connection, String path, long offset) {
            this.ctx = ctx;
            this.connection = connection;
            this.path = path;
            this.offset = offset;
        }

        private InputStream delegate() throws IOException {
            if (delegate != null) {
                return delegate;
            }
            FileObject file;
            try {
                FileSystemHandler handler = Utils.getFileSystemHandler(connection);
                file = handler.resolveFileWithSuspension(handler.getBaseDirectoryPath() + path);
                if (!file.exists()) {
                    throw new IOException("file not found on connection '" + connection + "': " + path);
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException("cannot open '" + path + "' on connection '" + connection + "'", e);
            }

            ctx.resources().register(NAME + ":file", file::close);

            // Identity is the connection name and the configured path, never a resolved URI: it is
            // written into the run's workspace on a shared mount and printed when a guard mismatch is
            // logged, so a URI carrying userinfo would publish credentials in both places.
            ctx.job().sourceOpened(file.getContent().getSize(),
                    file.getContent().getLastModifiedTime());

            InputStream opened = file.getContent().getInputStream();
            if (offset > 0L) {
                long skipped = 0L;
                while (skipped < offset) {
                    long n = opened.skip(offset - skipped);
                    if (n <= 0L) {
                        throw new IOException("cannot skip to offset " + offset + " in '" + path
                                + "': the file is only " + skipped + " bytes past the start");
                    }
                    skipped += n;
                }
            }
            delegate = ctx.resources().register(NAME + ":stream", opened);
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
}
