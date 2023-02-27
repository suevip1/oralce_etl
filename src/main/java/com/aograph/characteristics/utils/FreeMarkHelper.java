package com.aograph.characteristics.utils;

import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapper;
import freemarker.template.Template;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

public class FreeMarkHelper {

    private static final Configuration cfg = new Configuration();
    static {
        cfg.setObjectWrapper(new DefaultObjectWrapper());
        cfg.setClassicCompatible(true);
    }

    public static String convert(String content, Map<String, ?> values) {
        try {
            Template template = new Template("name", new StringReader(content), cfg);

            Writer out = new StringWriter();
            template.process(values, out);
            out.flush();

            return out.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
