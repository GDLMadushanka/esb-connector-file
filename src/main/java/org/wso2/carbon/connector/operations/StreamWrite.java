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
import org.apache.synapse.stream.StreamCommit;
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
 * Tail of a stream pipeline: pulls the chain to completion and writes it to a file.
 *
 * <pre>
 *   &lt;file.streamWrite configKey="conn1"&gt;
 *       &lt;path&gt;out/big.csv&lt;/path&gt;
 *   &lt;/file.streamWrite&gt;
 * </pre>
 *
 * <p>This is the only operator in the chain that reads, so the entire transfer happens inside
 * {@link #consume}. The pipeline holds this operation's parameters bound for that whole call, unlike a
 * source or transform where they are bound only for the build.
 *
 * <h2>Writes to a temporary name and renames on success</h2>
 * A partial file sitting at the destination is the worst available outcome: it looks complete, so a
 * later run reads it as complete and nothing reports a problem. So the output goes to
 * {@code <path>.part} and is moved into place only when the run succeeded — {@link StreamCommit}'s
 * {@code close()} publishes, {@code abort()} discards, and the pipeline calls one or the other
 * depending on the outcome.
 */
public class StreamWrite extends AbstractConnector implements StreamSink {

    private static final String NAME = "file.streamWrite";

    private static final int BUFFER_BYTES = 64 * 1024;

    /** How often to report progress and check for cancellation, in bytes. */
    private static final long CHECK_INTERVAL_BYTES = 8L * 1024 * 1024;

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        throw new ConnectException(NAME + " is only valid inside a <streamPipeline>. It consumes a lazy"
                + " chain rather than mediating a message, so there is nothing for it to do in a"
                + " sequence. Use file.write instead.");
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void consume(InputStream upstream, StreamContext ctx) throws StreamException, IOException {

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

        FileObject target;
        FileObject part;
        try {
            FileSystemHandler handler = Utils.getFileSystemHandler(connection);
            String base = handler.getBaseDirectoryPath();
            target = handler.resolveFileWithSuspension(base + path);
            part = handler.resolveFileWithSuspension(base + path + ".part");
            FileObject parent = target.getParent();
            if (parent != null && !parent.exists()) {
                parent.createFolder();
            }
        } catch (Exception e) {
            throw new StreamException("cannot open target '" + path + "' on connection '"
                    + connection + "'", e, true);
        }

        ctx.resources().register(NAME + ":target", target::close);
        ctx.resources().register(NAME + ":part", part::close);

        OutputStream out = part.getContent().getOutputStream();

        // Registered as committing, so the run's outcome decides whether this is published. A close
        // failure also fails the transfer: every byte may have been read, but the destination is not in
        // the state the caller was told it would be.
        ctx.resources().registerCommitting(NAME + ":commit", new RenameOnCommit(out, part, target));

        byte[] buffer = new byte[BUFFER_BYTES];
        long written = 0L;
        long sinceCheck = 0L;
        int read;

        while ((read = upstream.read(buffer)) != -1) {
            out.write(buffer, 0, read);
            written += read;
            sinceCheck += read;

            if (sinceCheck >= CHECK_INTERVAL_BYTES) {
                sinceCheck = 0L;
                // Records are not knowable here: a sink sees a byte chain, not a structure.
                ctx.job().progress(ctx.stageName(), written, 0L);

                // Cooperative, and checked between buffers rather than per byte, so a stop lands on a
                // boundary this writer chose. A cancelled run aborts, so the part file is discarded.
                if (ctx.job().isCancelled()) {
                    throw new StreamException("transfer to '" + path + "' was cancelled after "
                            + written + " bytes", false);
                }
            }
        }

        ctx.job().progress(ctx.stageName(), written, 0L);
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
