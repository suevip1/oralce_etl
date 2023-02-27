package com.aograph.characteristics.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpringContextUtil implements ApplicationContextAware {

    private static ApplicationContext context = null;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException {
        this.context = applicationContext;
    }

    public static <T> T getBean(String beanName) {
        return (T) context.getBean(beanName);
    }

    public static int getInt(String prop, int def) {
        return context.getEnvironment().getProperty(prop, Integer.class, def);
    }

    public static String getString(String prop, String def) {
        return context.getEnvironment().getProperty(prop, String.class, def);
    }

    public static boolean getBoolean(String prop, boolean def) {
        return context.getEnvironment().getProperty(prop, Boolean.class, def);
    }

    public static <T> T getBean(Class<T> clazz) {
        return (T) context.getBean(clazz);
    }

    /// 获取当前环境
    public static String getActiveProfile() {
        return context.getEnvironment().getActiveProfiles()[0];
    }
}
