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

package org.wso2.carbon.connector.pojo;

import org.apache.commons.lang.StringUtils;
import org.wso2.carbon.connector.exception.InvalidConfigurationException;

/**
 * Configs needed for SFTP connection
 * Referece: https://commons.apache.org/proper/commons-vfs/commons-vfs2/apidocs/org/apache/commons/vfs2/provider/sftp/SftpFileSystemConfigBuilder.html
 */
public class SFTPConnectionConfig extends RemoteServerConfig {

    //Jsch connection timeout
    private int connectionTimeout;

    //Jsch session timeout
    private int sessionTimeout;
    /**
     * Host key checking to use
     * Possible values yes, no, ask
     * https://winscp.net/eng/docs/ssh_verifying_the_host_key
     */
    private boolean strictHostKeyChecking;

    //the identity files (your private key files).
    private String privateKeyFilePath;

    //Passphrase of the private key
    private String privateKeyPassword;

    private String setAvoidPermission;
    private boolean sftpPathFromRoot = false;

    /**
     * Whether to skip the probe that works out if the server offers a shell.
     *
     * <p><b>Defaults to true, which is not the VFS default</b>, because the VFS default can hang the
     * connection for ever. On construction the provider runs {@code id -u} over an exec channel to
     * decide how to model POSIX permissions. A managed SFTP service -- SFTPCloud, AWS Transfer,
     * Azure's SFTP endpoint, most appliances -- serves SFTP and nothing else: it accepts the channel
     * and never answers, and the read that waits for the answer has no timeout. The connection does
     * not fail, it stops, and a transfer waiting behind it stops with it.
     *
     * <p>What skipping it costs is small and local: VFS then assumes the connecting user owns what it
     * reads, instead of comparing the file's uid against the one {@code id -u} would have returned.
     * That is a client-side guess either way -- the server enforces the real permissions and rejects
     * what is not allowed -- so the trade is a slightly more permissive pre-check against an
     * unbounded hang.
     *
     * <p>Set it false for a server that does offer a shell and where the uid-based check is wanted.
     */
    private boolean disableDetectExecChannel = true;

    /**
     * Create a SFTPConnectionConfig
     * with default values set
     */
    public SFTPConnectionConfig() {
        this.strictHostKeyChecking = false;
        this.connectionTimeout = 100000;
        this.sessionTimeout = 150000;
        this.setAvoidPermission = "true";
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(String connectionTimeout) throws InvalidConfigurationException {
        if (StringUtils.isNotEmpty(connectionTimeout)) {
            if (StringUtils.isNumeric(connectionTimeout)) {
                this.connectionTimeout = Integer.parseInt(connectionTimeout);
            } else {
                throw new InvalidConfigurationException("Parameter 'connectionTimeout' does not contain a numeric value");
            }
        }
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(String sessionTimeout) throws InvalidConfigurationException {
        if (StringUtils.isNotEmpty(sessionTimeout)) {
            if (StringUtils.isNumeric(sessionTimeout)) {
                this.sessionTimeout = Integer.parseInt(sessionTimeout);
            } else {
                throw new InvalidConfigurationException("Parameter 'sessionTimeout' does not contain a numeric value");
            }
        }
    }

    public boolean isStrictHostKeyChecking() {
        return strictHostKeyChecking;
    }

    public void setStrictHostKeyChecking(String strictHostKeyChecking) {
        if (StringUtils.isNotEmpty(strictHostKeyChecking)) {
            this.strictHostKeyChecking = Boolean.parseBoolean(strictHostKeyChecking);
        }
    }

    public String getPrivateKeyFilePath() {
        return privateKeyFilePath;
    }

    public void setPrivateKeyFilePath(String privateKeyFilePath) {
        if (StringUtils.isNotEmpty(privateKeyFilePath)) {
            this.privateKeyFilePath = privateKeyFilePath;
        }
    }

    public String getPrivateKeyPassword() {
        return privateKeyPassword;
    }

    public void setPrivateKeyPassword(String privateKeyPassword) {
        if (StringUtils.isNotEmpty(privateKeyPassword)) {
            this.privateKeyPassword = privateKeyPassword;
        }
    }

    public String getAvoidPermissionCheck() {
        return setAvoidPermission;
    }

    public void setAvoidPermissionCheck(String setAvoidPermission) {
        if (StringUtils.isNotBlank(setAvoidPermission)) {
            this.setAvoidPermission = setAvoidPermission;
        }
    }

    public boolean isSftpPathFromRoot() {
        return sftpPathFromRoot;
    }

    public void setSftpPathFromRoot(String sftpPathFromRoot) {
        if (StringUtils.isNotEmpty(sftpPathFromRoot)) {
            this.sftpPathFromRoot = Boolean.parseBoolean(sftpPathFromRoot);
        }
    }

    public boolean isDisableDetectExecChannel() {
        return disableDetectExecChannel;
    }

    public void setDisableDetectExecChannel(String disableDetectExecChannel) {
        if (StringUtils.isNotEmpty(disableDetectExecChannel)) {
            this.disableDetectExecChannel = Boolean.parseBoolean(disableDetectExecChannel);
        }
    }
}
