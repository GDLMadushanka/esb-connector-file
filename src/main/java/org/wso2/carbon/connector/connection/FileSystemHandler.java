/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
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

package org.wso2.carbon.connector.connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.connector.utils.Const;
import org.wso2.org.apache.commons.vfs2.provider.FileProvider;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.VFS;
import org.wso2.org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileSystemManager;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;
import org.wso2.org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.wso2.org.apache.commons.vfs2.cache.DefaultFilesCache;
import org.wso2.org.apache.commons.vfs2.cache.NullFilesCache;
import org.wso2.integration.connector.core.connection.ConnectionConfig;
import org.wso2.carbon.connector.exception.FileServerConnectionException;
import org.wso2.integration.connector.core.connection.Connection;
import org.wso2.carbon.connector.filelock.FileLockManager;
import org.wso2.carbon.connector.pojo.ConnectionConfiguration;
import org.wso2.carbon.connector.connection.FileServerProtocol;

/**
 * Object that handles all file operations
 * attached to the connection. This contains
 * FileSystemManager with FileSystemOptions
 * configured.
 */
public class FileSystemHandler implements Connection {

    private static final Log log = LogFactory.getLog(FileSystemHandler.class);

    private static final String SMB2_PROVIDER_CLASS =
            "org.wso2.org.apache.commons.vfs2.provider.smb2.Smb2FileProvider";

    private FileSystemManager fsManager;
    private FileSystemOptions fsOptions;
    private FileLockManager fileLockManager;
    private VFSConnectionWrapper vfsWrapper;
    private ConnectionConfiguration connectionConfig;

    /**
     * URL constructed adding file protocol,
     * username, password and the base directory path
     * [protocol prefix]://[ username[: password]@] hostname[: port][ base-path]
     */
    private String baseDirectoryPath;

    /**
     * Create a new FileSystemHandler object. This will contain a new FileSystemManager
     * and File System Options.
     *
     * @param fsConfig Connection config object
     * @throws FileServerConnectionException in case of an error setting FileSystemManager
     */
    public FileSystemHandler(ConnectionConfiguration fsConfig) throws FileServerConnectionException {
        try {
            this.connectionConfig = fsConfig;

            this.fsManager = new StandardFileSystemManager();

            // Configure VFS caching at FileSystemManager level based on user preference
            if (fsConfig.isFileCacheEnabled()) {
                ((StandardFileSystemManager) fsManager).setFilesCache(new DefaultFilesCache());
                if (log.isDebugEnabled()) {
                    log.debug("VFS file caching enabled using DefaultFilesCache");
                }
            } else {
                ((StandardFileSystemManager) fsManager).setFilesCache(new NullFilesCache());
                if (log.isDebugEnabled()) {
                    log.debug("VFS file caching disabled using NullFilesCache");
                }
            }

            ((StandardFileSystemManager) fsManager).init();
            ensureSmb2Provider((StandardFileSystemManager) fsManager);
            this.fsOptions = new FileSystemOptions();
            setupFSO(fsOptions, fsConfig);
            selfTest((StandardFileSystemManager) fsManager, baseDirectoryPath, fsOptions);
            fileLockManager = new FileLockManager(this, fsConfig.isClusterLockingEnabled());
        } catch (FileSystemException e) {
            String errorMsg = "Unable to create FileSystemManager: " + e.getMessage();
            log.error(errorMsg, e);
            throw new FileServerConnectionException(errorMsg, e);
        }
    }

    /**
     * Registers the SMB2 provider when the runtime has not already done it.
     *
     * <p>That provider lives in {@code commons-vfs2-sandbox} and announces itself through a
     * {@code META-INF/vfs-providers.xml}, which {@link StandardFileSystemManager#init()} finds by
     * asking its <b>own</b> classloader for every copy of that resource. On a flat classpath the
     * sandbox jar sits beside the core one and is found. Under OSGi it is a separate bundle, the scan
     * does not cross bundles, and the scheme is simply never registered.
     *
     * <p>Nothing announces that. The first {@code smb2://} URL then fails with
     * <i>"Could not find file with URI ... because it is a relative path, and no base URI was
     * provided"</i> — an unresolved-scheme message that names neither SMB nor a missing provider, and
     * that arrives from the connection pool as "ValidateObject failed". Registering it here costs one
     * call and removes a failure nobody could read.
     *
     * <p>Failure to register is logged rather than thrown: the sandbox is an optional dependency, and
     * a deployment that uses no SMB connection should not be stopped by its absence. A deployment that
     * does use one fails at the connection instead, which is where it can be understood.
     */
    private static void ensureSmb2Provider(StandardFileSystemManager manager) {
        if (manager.hasProvider(Const.SMB2_SCHEME)) {
            return;
        }
        try {
            // Loaded through the MANAGER's classloader rather than this class's, so the provider and
            // the manager are always the same build of VFS.
            ClassLoader loader = manager.getClass().getClassLoader();
            Class<?> type = loader.loadClass(SMB2_PROVIDER_CLASS);
            manager.addProvider(Const.SMB2_SCHEME, newProvider(type));
            registerOnTheSingleton(type);
            if (log.isDebugEnabled()) {
                log.debug("Registered the smb2 VFS provider, which this runtime had not discovered.");
            }
        } catch (ReflectiveOperationException | FileSystemException | LinkageError smb2Unavailable) {
            log.warn("The smb2 VFS provider could not be registered, so SMB2 connections will not"
                    + " resolve. Check that commons-vfs2-sandbox and smbj are on the runtime.",
                    smb2Unavailable);
        }
    }

    /**
     * Registers the provider on {@link VFS#getManager()} as well, which is not redundant.
     *
     * <p>{@code Smb2FileNameParser} parses a URL through the two-argument
     * {@code HostFileNameParser.extractToPath(uri, name)}, which has no component context. With no
     * context, that method takes its list of known schemes from <b>the static VFS singleton</b>, not
     * from the manager the provider was registered on:
     *
     * <pre>
     *   fsm = (context != null) ? context.getFileSystemManager() : VFS.getManager();
     *   scheme = UriParser.extractScheme(fsm.getSchemes(), uri, name);
     * </pre>
     *
     * <p>So a manager that knows {@code smb2} perfectly well still cannot parse an {@code smb2://}
     * URL unless the singleton knows it too. The scheme is not stripped, the remainder no longer
     * starts with {@code //}, and the failure surfaces three frames up as <i>"Invalid absolute
     * URI"</i> — a message about the URL, when the URL was never the problem.
     *
     * <p>On a flat classpath this never shows: the singleton runs the same provider scan and finds
     * the sandbox jar by itself. It only appears where that scan cannot see the provider, which is
     * exactly OSGi.
     */
    private static void registerOnTheSingleton(Class<?> providerType)
            throws ReflectiveOperationException, FileSystemException {
        FileSystemManager shared = VFS.getManager();
        if (shared instanceof DefaultFileSystemManager && !shared.hasProvider(Const.SMB2_SCHEME)) {
            // A second instance: a provider belongs to one manager, and both need their own.
            ((DefaultFileSystemManager) shared).addProvider(Const.SMB2_SCHEME,
                    newProvider(providerType));
        }
    }

    private static FileProvider newProvider(Class<?> type) throws ReflectiveOperationException {
        return (FileProvider) type.getDeclaredConstructor().newInstance();
    }

    /**
     * Resolves the base path once, here, where everything about this connection is still in hand.
     *
     * <p>Diagnostic only, and it never fails the connection. A resolve that goes wrong later surfaces
     * from the pool as "ValidateObject failed", several layers from anything that explains it; doing
     * it here says whether the manager that was just built can reach its own base path, and prints
     * the scheme table next to the answer when it cannot.
     */
    private static void selfTest(StandardFileSystemManager manager, String basePath,
                                 FileSystemOptions options) {
        if (basePath == null) {
            return;
        }
        try (FileObject base = manager.resolveFile(basePath, options)) {
            if (log.isDebugEnabled()) {
                log.debug("connection self-test: resolved its base path, exists=" + base.exists());
            }
        } catch (Exception cannotResolve) {
            log.warn("connection self-test: could NOT resolve its own base path."
                    + " hasProvider(smb2)=" + manager.hasProvider(Const.SMB2_SCHEME)
                    + " schemes=" + java.util.Arrays.toString(manager.getSchemes())
                    + " baseFile=" + baseFileOf(manager), cannotResolve);
        }
    }

    /** The manager's base file, which is what a relative-looking URI would be resolved against. */
    private static String baseFileOf(StandardFileSystemManager manager) {
        try {
            return String.valueOf(manager.getBaseFile());
        } catch (Exception unknown) {
            return "unavailable";
        }
    }

    /** Where a class was actually loaded from, for telling two copies of VFS apart in a log. */
    private static String codeSourceOf(Class<?> type) {
        try {
            return String.valueOf(type.getProtectionDomain().getCodeSource().getLocation());
        } catch (RuntimeException unknown) {
            return "an unknown location";
        }
    }

    /**
     * Sets connection configs to VFS API.
     * Be careful as FTP, FTPS, SFTP variables looks almost same.
     *
     * @param fso      FileSystemOptions to set the configs
     * @param fsConfig Configuration DTO
     */
    private void setupFSO(FileSystemOptions fso, ConnectionConfiguration fsConfig)
            throws FileServerConnectionException {

        ProtocolBasedFileSystemSetup fileSystemSetup;

        switch (fsConfig.getProtocol()) {
            case LOCAL:
                fileSystemSetup = new LocalFileSystemSetup();
                break;
            case FTP:
                fileSystemSetup = new FTPFileSystemSetup();
                break;
            case FTPS:
                fileSystemSetup = new FTPSFileSystemSetup();
                break;
            case SFTP:
                fileSystemSetup = new SFTPFileSystemSetup();
                break;
            case SMB:
                fileSystemSetup = new SMBFileSystemSetup();
                break;
            case SMB2:
                fileSystemSetup = new SMB2FileSystemSetup();
                break;
            default:
                throw new IllegalStateException("Unexpected protocol value: " + fsConfig.getProtocol());
        }

        baseDirectoryPath = fileSystemSetup.setupFileSystemHandler(fso, fsConfig);
    }

    /**
     * Get FileSystemManager object configured for the connection
     *
     * @return FileSystemManager
     */
    public FileSystemManager getFsManager() {
        return fsManager;
    }

    /**
     * Get FileSystemOptions object configured for the connection
     *
     * @return FileSystemOptions object
     */
    public FileSystemOptions getFsOptions() {
        return fsOptions;
    }

    /**
     * Get base directory path for the connection.
     * [protocol prefix]://[ username[: password]@] hostname[: port][ base-path]
     *
     * @return base directory path as String
     */
    public String getBaseDirectoryPath() {
        return baseDirectoryPath;
    }

    /**
     * Get FileLockManager used to lock files
     * on the connection.
     *
     * @return FileLockStore
     */
    public FileLockManager getFileLockManager() {
        return fileLockManager;
    }

    /**
     * Initialize VFS connection wrapper for suspension-enabled protocols
     *
     * @param connectionName Name of the connection
     */
    public void initializeVFSWrapper(String connectionName) {
        // Only initialize wrapper for protocols that need suspension
        if (needsSuspensionSupport()) {
            this.vfsWrapper = new VFSConnectionWrapper(fsManager, fsOptions, connectionConfig, connectionName);
            if (log.isDebugEnabled()) {
                log.debug("VFS connection wrapper initialized for connection: " + connectionName);
            }
        }
    }

    /**
     * Resolve file with suspension logic (for FTP/FTPS) or direct VFS (for others)
     *
     * @param path File path to resolve
     * @return FileObject
     * @throws FileSystemException if file resolution fails
     */
    public org.wso2.org.apache.commons.vfs2.FileObject resolveFileWithSuspension(String path) throws FileSystemException {
        if (vfsWrapper != null && needsSuspensionSupport()) {
            // Custom suspension logic for FTP/FTPS (bypass WSO2 ConnectionHandler)
            return vfsWrapper.resolveFile(path);
        } else {
            // Direct VFS - SFTP/SMB already have suspension via WSO2 ConnectionHandler
            // LOCAL doesn't need suspension
            return fsManager.resolveFile(path, fsOptions);
        }
    }

    /**
     * Check if current protocol needs suspension support
     * All remote protocols benefit from suspension logic for connection failures
     */
    private boolean needsSuspensionSupport() {
        // All protocols except LOCAL need suspension support
        return connectionConfig.getProtocol() != FileServerProtocol.LOCAL;
    }

    /**
     * Get VFS connection wrapper for monitoring
     */
    public VFSConnectionWrapper getVfsWrapper() {
        return vfsWrapper;
    }

    @Override
    public void connect(ConnectionConfig config) {
        throw new UnsupportedOperationException("Nothing to do when connect");
    }

    @Override
    public void close() {
        ((StandardFileSystemManager) fsManager).close();
        fileLockManager.releaseAllLocks();

        // Cleanup VFS wrapper state if exists
        if (vfsWrapper != null) {
            // Remove connection state to prevent memory leaks
            VFSConnectionWrapper.removeConnectionState(vfsWrapper.getConnectionName());
        }
    }

}
