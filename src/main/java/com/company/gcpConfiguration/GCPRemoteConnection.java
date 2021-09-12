package com.company.gcpConfiguration;

import java.util.Properties;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class GCPRemoteConnection {

	private static Session sessionOfDB = null;
	private static ChannelSftp sftpChannelOfDB = null;

	private static Session createSessionToConnect() {

		Session sessionToConnect = null;
		try {
			JSch jsch = new JSch();
			jsch.setKnownHosts(GCPConfiguration.KNOWN_HOSTS);

			Properties config = new Properties();
			config.put("StrictHostKeyChecking", "no");

			sessionToConnect = jsch.getSession(GCPConfiguration.REMOTE_USER, GCPConfiguration.REMOTE_HOST);
			jsch.addIdentity(GCPConfiguration.KEY);
			sessionToConnect.setConfig(config);
			sessionToConnect.connect();
		} catch (JSchException e) {
			e.printStackTrace();
		}
		return sessionToConnect;
	}

	private static ChannelSftp createChannelToConnect() {
		if (null == sessionOfDB) {
			sessionOfDB = createSessionToConnect();
		}

		if (null == sessionOfDB) {
			return null;
		}

		if (null == sftpChannelOfDB) {
			try {
				ChannelSftp channel = (ChannelSftp) sessionOfDB.openChannel("sftp");
				channel.connect();
				sftpChannelOfDB = (ChannelSftp) channel;
			} catch (JSchException exception) {
				exception.printStackTrace();
			}
		}
		return sftpChannelOfDB;
	}

	public static ChannelSftp getsftpChannelOfDBToConnect() {
		return createChannelToConnect();
	}

	public static void closeSession() {
		if (null != sftpChannelOfDB) {
			sftpChannelOfDB.disconnect();
		}
		if (null != sessionOfDB) {
			sessionOfDB.disconnect();
		}
	}
}