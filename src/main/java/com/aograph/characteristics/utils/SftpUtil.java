package com.aograph.characteristics.utils;

import com.jcraft.jsch.*;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.sftp.SftpClientFactory;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class SftpUtil extends FtpUtil {
    private ChannelSftp command;  
    
    private Session session;  
    
    private String authentication;	// = "publickey"; 
    private String certificate;						// test.ppk
    private String certificateUser;
    private String certificatePassword;
    
    @Override
	public boolean connect() throws IOException {
        //If the client is already connected, disconnect  
        if (command != null) {  
            close();  
        } 
        
        FileSystemOptions fso = new FileSystemOptions();
        if (authentication != null) {
        	SftpFileSystemConfigBuilder.getInstance().setPreferredAuthentications(fso, authentication);
        }
        SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(fso, "no");  
        if (certificate != null) {
        	SftpFileSystemConfigBuilder.getInstance().setIdentities(fso, new File[]{new File(certificate)});
        }
        if (certificateUser != null) {
        	SftpFileSystemConfigBuilder.getInstance().setUserInfo(fso, new MyUserInfo(certificateUser, certificatePassword));
        }
        session = SftpClientFactory.createConnection(server, port, user == null ? null : user.toCharArray(), password == null ? null : password.toCharArray(), fso);  
        try {
	        Channel channel = session.openChannel("sftp");  
	        channel.connect();
	        
	        command = (ChannelSftp) channel;  
        } catch (JSchException e) {
        	throw new IOException(e);
        }
        return command.isConnected(); 
	}

	@Override
	public void close() throws IOException {
        if (command != null) {  
            command.exit();  
        }  
        if (session != null) {  
            session.disconnect();  
        }  
        command = null;  
	}

    public boolean isConnected() {
        if (command != null) {
            return command.isConnected();
        }

        if (session != null) {
            return session.isConnected();
        }

        return false;
    }

	@Override
	public boolean changeDirectory(String path) throws IOException {
        try {  
            command.cd(path);  
        } catch (SftpException e) {  
            throw new IOException(e);  
        }  
        return true;  
	}

    public boolean createDirectory(String dirName) {  
        try {  
            command.mkdir(dirName);  
        } catch (SftpException e) {  
            return false;  
        }  
        return true;  
    }  
  
	@Override
	public boolean removeDirectory(String path) throws IOException {
        try {  
            command.rmdir(path);  
        } catch (SftpException e) {  
            return false;  
        }  
        return true;  
	}

	@Override
	public boolean removeDirectory(String path, boolean isAll) throws IOException {
        try {  
            command.rmdir(path);
        } catch (SftpException e) {  
            return false;  
        }  
        return true;  
	}

	@Override
	public boolean isFileExist(String file) throws IOException {
        try {  
            return command.get(file) != null;  
        } catch (SftpException e) {  
            return false;
        }  
	}

	@Override
	public List<String> listFiles(String path) throws IOException {
		try {
	        Vector<LsEntry> rs = command.ls(path);  
	        List<String> result = new ArrayList<String>();  
	        for (int i = 0; i < rs.size(); i++) {  
                result.add(rs.get(i).getFilename());  
	        }  
	        return result;
		} catch (SftpException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean deleteFile(String pathName) throws IOException {
        try {  
            command.rm(pathName);  
        } catch (SftpException e) {  
        	throw new IOException(e);
        }
        return false;  
	}

	@Override
	public boolean uploadFile(String fileName) throws IOException {
		return uploadFile(fileName, fileName);
	}

	@Override
	public boolean uploadFile(String fileName, String newName) throws IOException {
        FileInputStream fis = new FileInputStream(fileName); 
        return uploadFile(fis, newName);
	}

	public boolean uploadFile(InputStream is, String newName) throws IOException {
        try {          	
            command.put(is, newName+".temp");
            command.rename(newName+".temp", newName);
        } catch (SftpException e) {  
            e.printStackTrace();  
            throw new IOException(e);
        } finally {  
            if (is != null) {  
                is.close();  
            }  
        }
        return true;
	}
	
	public void rename(String name, String newName) throws IOException {
        try {  
            command.rename(name, newName);  
        } catch (SftpException e) {  
            e.printStackTrace();  
        }
	}
	
    protected boolean downloadFileAfterCheck(String remotePath, String localPath) throws IOException {  
        FileOutputStream outputSrr = null;  
        try {  
            File file = new File(localPath);  
            if (!file.exists()) {  
                outputSrr = new FileOutputStream(localPath);  
                command.get(remotePath, outputSrr);  
            }  
        } catch (SftpException e) {  
        	try {
				throw new IOException(remotePath + " not found in " + command.pwd(), e);
			} catch (SftpException e1) {
//				e1.printStackTrace();
			}
			return false;
        } finally {  
            if (outputSrr != null) {  
                outputSrr.close();  
            }  
        }  
        return true;  
    }  
  
    public boolean download(String remotePath, String localPath) throws IOException {  
        FileOutputStream fos = new FileOutputStream(localPath);  
        try {  
            command.get(remotePath, fos);  
        } catch (SftpException e) {
        	try {
				throw new IOException(remotePath + " not found in " + command.pwd(), e);
			} catch (SftpException e1) {
//				e1.printStackTrace();
			}
			return false;
        } finally {  
            if (fos != null) {  
                fos.close();  
            }  
        }  
        return true;  
    }  
  
    public boolean isDirectory(String path) {  
        try {  
            return command.stat(path).isDir();  
        } catch (SftpException e) {  
            //e.printStackTrace();  
        }  
        return false;  
    }  
  
    public String getWorkDir() {  
        try {  
            return command.pwd();  
        } catch (SftpException e) {  
            e.printStackTrace();  
        }  
        return null;  
    }  

    public String getAuthentication() {
		return authentication;
	}

	public void setAuthentication(String authentication) {
		this.authentication = authentication;
	}

	public String getCertificate() {
		return certificate;
	}

	public void setCertificate(String certificate) {
		this.certificate = certificate;
	}

	public String getCertificateUser() {
		return certificateUser;
	}

	public void setCertificateUser(String certificateUser) {
		this.certificateUser = certificateUser;
	}

	public String getCertificatePassword() {
		return certificatePassword;
	}

	public void setCertificatePassword(String certificatePassword) {
		this.certificatePassword = certificatePassword;
	}


	public static class MyUserInfo implements UserInfo {
    	String passphrase;
    	String password;
    	
    	MyUserInfo(String passphrase, String password) {
    		this.passphrase = passphrase;
    		this.password = password;
    	}
    	
		@Override
		public String getPassphrase() {
			return passphrase;
		}

		@Override
		public String getPassword() {
			return password;
		}

		@Override
		public boolean promptPassword(String message) {
			return false;
		}

		@Override
		public boolean promptPassphrase(String message) {
			return true;
		}

		@Override
		public boolean promptYesNo(String message) {
			return false;
		}

		@Override
		public void showMessage(String message) {
		}    	
    }
    
	public static void main(String[] args) throws Exception {
		SftpUtil ftp = new SftpUtil();

        ftp.setServer("172.16.0.197");
        ftp.setPort(22);
        ftp.setUser("dongfang");
        ftp.setPassword("wkB1h@lPJl");
		ftp.connect();

//		ftp.changeDirectory("/orders");
		List files = ftp.listFiles("/");
		System.out.println("files:"+files);
//		
		boolean b = ftp.isFileExist("/orders/MERGE_10008723@supplier.cn.tesco.com_20121001143138.txt");
		System.out.println("b:"+b);
//		
//		ftp.download("/orders/MERGE_10008723@supplier.cn.tesco.com_20121001143138.txt", "c:/test112.txt");
//		
//		boolean b = ftp.deleteFile("/orders/MERGE_10008723@supplier.cn.tesco.com_20121001075110.txt.done");
//		System.out.println("deleteFile:"+b);
//		
		b = ftp.uploadFile(new ByteArrayInputStream("test".getBytes()), "/Tesco_Inbound_OB/test.txt");
		System.out.println("uploadFile:"+b);
		
	}
}
