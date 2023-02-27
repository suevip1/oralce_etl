package com.aograph.characteristics.jobHandler;

// 文件选取
public interface Filter {
    // 文件是否可用
    boolean match(String name);

    // 文件类型
    String getType();
}
