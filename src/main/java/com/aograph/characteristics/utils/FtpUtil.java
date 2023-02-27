package com.aograph.characteristics.utils;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FtpUtil {
	private FTPClient ftpClient;
	public static final int BINARY_FILE_TYPE = FTP.BINARY_FILE_TYPE;
	public static final int ASCII_FILE_TYPE = FTP.ASCII_FILE_TYPE;

	protected String server;
	protected int port = 0;
	protected String user;
	protected String password;
	protected String workDir = "/";
	static Logger logger = LoggerFactory.getLogger(FtpUtil.class);

	// path should not the path from root index
	// or some FTP server would go to root as '/'.
	public boolean connect() throws IOException {
		ftpClient = new FTPClient();
		ftpClient.connect(server, port);
		ftpClient.setControlEncoding("GBK");
		// ftpClient.setControlEncoding("UTF-8");
		boolean b = ftpClient.login(user, password);
		// System.out.println(ftpClient.getReplyCode());
		int reply = ftpClient.getReplyCode();
		ftpClient.setDataTimeout(120000);

		if (!FTPReply.isPositiveCompletion(reply)) {
			ftpClient.disconnect();
			System.err.println("FTP server refused connection.");
			return false;
		}
		// Path is the sub-path of the FTP path
		if (workDir.length() != 0) {
			ftpClient.changeWorkingDirectory(workDir);
		}
		
		ftpClient.enterLocalPassiveMode();
		
		return b;
	}

	// FTP.BINARY_FILE_TYPE | FTP.ASCII_FILE_TYPE
	// Set transform type
	public void setFileType(int fileType) throws IOException {
		ftpClient.setFileType(fileType);
	}

	public void close() throws IOException {
		if (ftpClient.isConnected()) {
			ftpClient.disconnect();
		}
	}

	// =======================================================================
	// == About directory =====
	// The following method using relative path better.
	// =======================================================================

	public boolean changeDirectory(String path) throws IOException {
		return ftpClient.changeWorkingDirectory(path);
	}

	public boolean createDirectory(String pathName) throws IOException {
		return ftpClient.makeDirectory(pathName);
	}

	public boolean removeDirectory(String path) throws IOException {
		return ftpClient.removeDirectory(path);
	}

	// delete all subDirectory and files.
	public boolean removeDirectory(String path, boolean isAll) throws IOException {

		if (!isAll) {
			return removeDirectory(path);
		}

		FTPFile[] ftpFileArr = ftpClient.listFiles(path);
		if (ftpFileArr == null || ftpFileArr.length == 0) {
			return removeDirectory(path);
		}
		//
		for (FTPFile ftpFile : ftpFileArr) {
			String name = ftpFile.getName();
			if (ftpFile.isDirectory()) {
				System.out.println("* [sD]Delete subPath [" + path + "/" + name + "]");
				removeDirectory(path + "/" + name, true);
			} else if (ftpFile.isFile()) {
				System.out.println("* [sF]Delete file [" + path + "/" + name + "]");
				deleteFile(path + "/" + name);
			} else if (ftpFile.isSymbolicLink()) {

			} else if (ftpFile.isUnknown()) {

			}
		}
		return ftpClient.removeDirectory(path);
	}

	// fileName is the name relative to work path
	public boolean isFileExist(String file) throws IOException {
		if (file.indexOf("/") < 0) {
			return false;
		}

		String parent = file.substring(0, file.lastIndexOf("/"));
		if (parent.length() == 0) {
			parent = "/";
		}
		boolean flag = false;

		FTPFile[] ftpFileArr = ftpClient.listFiles(parent);
		for (FTPFile ftpFile : ftpFileArr) {
			String fileName = file.substring(file.lastIndexOf("/") + 1);
			if (ftpFile.getName().equalsIgnoreCase(fileName)) {
				flag = true;
				break;
			}
		}
		return flag;
	}

	public List<String> listFiles(String path) throws IOException {
		// listFiles return contains directory and file, it's FTPFile instance
		// listNames() contains directory, so using following to filer
		// directory.
		// String[] fileNameArr = ftpClient.listNames(path);
		FTPFile[] ftpFiles = ftpClient.listFiles(path);

		List<String> retList = new ArrayList<String>();
		if (ftpFiles == null || ftpFiles.length == 0) {
			return retList;
		}
		for (FTPFile ftpFile : ftpFiles) {
			// if (ftpFile.isFile()) {
			retList.add(ftpFile.getName());
			// }
		}

		return retList;
	}

	// pathName is full name
	public boolean deleteFile(String pathName) throws IOException {
		return ftpClient.deleteFile(pathName);
	}

	public boolean uploadFile(String fileName, String newName) throws IOException {
		boolean flag = false;
		InputStream iStream = null;
		try {
			iStream = new FileInputStream(fileName);
			ftpClient.enterLocalPassiveMode();
			flag = ftpClient.storeFile(newName + ".temp", iStream);
			ftpClient.rename(newName + ".temp", newName);
		} catch (IOException e) {
			flag = false;
			return flag;
		} finally {
			if (iStream != null) {
				iStream.close();
			}
		}
		return flag;
	}

	public boolean uploadFile(String fileName) throws IOException {
		return uploadFile(fileName, fileName);
	}

	// newName is full path
	public boolean uploadFile(InputStream iStream, String newName) throws IOException {
		boolean flag = false;
		try {
			// can execute [OutputStream storeFileStream(String remote)]
			// Above method return's value is the local file stream.
			flag = ftpClient.storeFile(newName + ".temp", iStream);
			ftpClient.rename(newName + ".temp", newName);
		} catch (IOException e) {
			logger.warn("my caught errors", e);
			flag = false;
			return flag;
		} finally {
			if (iStream != null) {
				iStream.close();
			}
		}
		return flag;
	}

	public void rename(String name, String newName) throws IOException {
		ftpClient.rename(name, newName);
	}

	// remoteFileName is relative to work path
	public boolean download(String remoteFileName, String localFileName) throws IOException {
		boolean flag = false;
		File outfile = new File(localFileName);
		if (!outfile.getParentFile().exists()) {
			outfile.getParentFile().mkdirs();
		}
		if (!outfile.exists()) {
			outfile.createNewFile();
		}

		OutputStream oStream = null;
		try {
			oStream = new FileOutputStream(outfile);
			flag = ftpClient.retrieveFile(remoteFileName, oStream);
		} catch (IOException e) {
			flag = false;
			return flag;
		} finally {
			oStream.close();
		}
		return flag;
	}

	// public InputStream downloadFile(String sourceFileName) throws IOException
	// {
	// return ftpClient.retrieveFileStream(sourceFileName);
	// }

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getWorkDir() {
		return workDir;
	}

	public void setWorkDir(String workDir) {
		this.workDir = workDir;
	}

	public static void main(String[] args) throws Exception {
		FtpUtil ftp = new FtpUtil();

		ftp.setServer("172.16.0.197");
		ftp.setPort(22);
		ftp.setUser("dongfang");
		ftp.setPassword("wkB1h@lPJl");
		boolean b = ftp.connect();
		System.out.println("b:" + b);

		// ftp.changeDirectory("/orders");
		List files = ftp.listFiles("/");
		System.out.println("files:" + files);
		//
		// b =
		// ftp.isFileExist("/orders/MERGE_10008723@supplier.cn.tesco.com_20121001143138.txt");
		// System.out.println("b:"+b);
		//
		// ftp.download("/orders/MERGE_10008723@supplier.cn.tesco.com_20121001143138.txt",
		// "c:/test112.txt");
		//
		// boolean b =
		// ftp.deleteFile("/orders/MERGE_10008723@supplier.cn.tesco.com_20121001075110.txt.done");
		// System.out.println("deleteFile:"+b);
		//
//		ftp.createDirectory("test1");
//		ftp.changeDirectory("test1");
//		b = ftp.uploadFile(new ByteArrayInputStream("www".getBytes()), "test1.txt");
//		System.out.println("uploadFile:" + b);

	}
}