package com.aograph.characteristics.jobHandler;

import com.aograph.characteristics.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class AbstractFileJob {
    public void processFiles(Filter filter, boolean history) throws IOException {
        String fileType = filter.getType();
        String home = System.getProperty("user.dir");
        // 删除时间太久的备份文件
        removeStaleFiles(new File(home, "data/"+fileType));

        // 备份到这个目录下， 目录按类型，日期存放
        File processedDir = new File(home, "data/"+fileType+"/"+ DateTimeHelper.date2String(new Date(), AirlinkConst.TIME_DATE_FORMAT));
        if (!processedDir.exists()) {
            processedDir.mkdirs();
        }

        File tempDir = new File(home, "data/tmp");
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }

        SftpUtil ftp = new SftpUtil();

        ftp.setServer(SpringContextUtil.getString("ftp.host", null));
        ftp.setPort(SpringContextUtil.getInt("ftp.port", 21));
        ftp.setUser(SpringContextUtil.getString("ftp.security_name", null));
        ftp.setPassword(SpringContextUtil.getString("ftp.security_code", null));
        LogHelper.log("connect ftp");
        ftp.connect();
        LogHelper.log("ftp connected");

        String dir = history ? SpringContextUtil.getString("ftp.history_dir", "/history") : SpringContextUtil.getString("ftp.download_dir", "/MU_FILES");
        List<String> files = ftp.listFiles(dir);
        List<File> dataFiles = new ArrayList<>();
        for (String file : files) {
            if (".".equals(file) || "..".equals(file) || file.startsWith(".")) {
                continue;
            }

            if (!filter.match(file)) {
                continue;
            }

            File dataFile = new File(tempDir, file);
            LogHelper.log("downloading " + file);
            if (dataFile.exists()) {
                dataFile.delete();
            }
            if (!ftp.isConnected()) {
                ftp.connect();
            }
            ftp.download(dir+"/" + file, dataFile.getAbsolutePath());

            if (!isValidFile(dataFile, "UTF-8")) {
                LogHelper.log("invalid file: " + dataFile.getAbsolutePath());
                continue;
            }

            ftp.deleteFile(dir+"/" + file);
            LogHelper.log("downloaded to " + dataFile.getAbsolutePath());

            dataFiles.add(dataFile);
        }

        try {
            processFile(dataFiles);
        } finally {
            // 备份文件
            for (File dataFile : dataFiles) {
                FileUtils.copyFile(dataFile, new File(processedDir, dataFile.getName()));
                dataFile.delete();
            }
        }
    }

    private boolean isValidFile(File file, String charset) throws IOException {
        if (!file.exists() || !file.canRead()) {
            return false;
        }

        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "r");
            long len = raf.length();
            if (len == 0L) {
                return false;
            } else {
                if (len > 100) {
                    len -= 100;
                } else {
                    len = 0;
                }
                raf.seek(len);
                byte[] bytes = new byte[(int)(raf.length() - len)];
                raf.read(bytes);

                return new String(bytes, charset).contains("end of the file");
            }
        } catch (FileNotFoundException e) {
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (Exception e2) {
                }
            }
        }
        return false;
    }

    public void removeStaleFiles(File file) {
        File[] files = file.listFiles();
        if (files == null) {
            return;
        }

        // 最多保留30天数据
        String lastDate = DateTimeHelper.date2String(DateTimeHelper.getDateAfter(new Date(), -30), AirlinkConst.TIME_DATE_FORMAT);
        for (File chd : files) {
            if (chd.getName().compareTo(lastDate) <= 0) {
                try {
                    FileUtils.deleteDirectory(chd);
                    LogHelper.log("File deleted: "+file.getAbsolutePath());
                } catch (IOException e) {
                    log.error(chd.getAbsolutePath(), e);
                    LogHelper.log("Failed to delete "+file.getAbsolutePath()+": " + e.toString());
                }
           }
        }
    }

    public void processFile(List<File> files) throws IOException {
        for (File file : files) {
            try {
                LogHelper.log("process file: " + file.getName()+", size:" + file.length());
                processFile(file);
            } catch (IOException e) {
                LogHelper.log("Failed to processFile "+file.getName()+": " + e.toString());
                e.printStackTrace();
                log.error(file.getName(), e);
            }
        }
    }

    public void processFile(File file) throws IOException {

    }

    protected void createSequence(JdbcTemplate template, String table, String seq) {
        Integer max = template.queryForObject("select max(id) from "+table, Integer.class);
        if (max == null) {
            max = 1;
        } else {
            max++;
        }

        String sql = "CREATE SEQUENCE "+seq+" MINVALUE "+max+" MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 CACHE 200";
        try {
            template.update(sql);
        } catch (DataAccessException e) {
            // 序列已经存在
        }
    }

    protected JdbcTemplate getJdbcTemplate() {
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(SpringContextUtil.getBean("oracleDataSource"));
        return template;
    }

    protected void batchInsert(String sql, List<Map<String, Object>> list) {
        if (list == null || list.size() == 0) {
            return;
        }

        String[] names = sql.substring(sql.indexOf("(") + 1, sql.indexOf(")")).split(",");
        for (int i = 0; i < names.length; i++) {
            names[i] = names[i].trim();
        }

        batchInsert(sql, names, list);
    }

    protected void batchInsert(String sql, String[] names, List<Map<String, Object>> list) {
        if (list == null || list.size() == 0) {
            return;
        }

        JdbcTemplate template = getJdbcTemplate();
        template.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement stat, int i) throws SQLException {
                Map<String, Object> m = list.get(i);
                for (int k = 1 ; k <= names.length ; k++) {
                    Object val = m.get(names[k - 1]);
                    if (val instanceof String) {
                        stat.setString(k, (String)val);
                    } else if (val instanceof Integer) {
                        stat.setInt(k, (Integer) val);
                    } else if (val instanceof Float) {
                        stat.setFloat(k, (Float) val);
                    } else if (val instanceof Long) {
                        stat.setLong(k, (Long) val);
                    } else if (val instanceof Double) {
                        stat.setDouble(k, (Double) val);
                    } else if (val instanceof java.sql.Date) {
                        stat.setDate(k, (java.sql.Date)val);
                    } else if (val instanceof Timestamp) {
                        stat.setTimestamp(k, (Timestamp)val);
                    }
                }
            }

            @Override
            public int getBatchSize() {
                return list.size();
            }
        });
    }
}
