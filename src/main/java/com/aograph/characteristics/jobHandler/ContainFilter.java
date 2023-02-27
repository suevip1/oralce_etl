package com.aograph.characteristics.jobHandler;

// 按文件名是否含字符串简单匹配
public class ContainFilter implements Filter {
    private String type;
    public ContainFilter(String type) {
        this.type = type;
    }

    @Override
    public boolean match(String name) {
        return name.contains(type);
    }

    @Override
    public String getType() {
        return type;
    }
}
