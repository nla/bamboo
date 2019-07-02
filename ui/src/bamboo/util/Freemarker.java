package bamboo.util;

import bamboo.core.Config;
import com.google.gson.Gson;
import freemarker.core.HTMLOutputFormat;
import freemarker.ext.beans.BeansWrapper;
import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import spark.Request;

public class Freemarker {
    private static final Gson gson = new Gson();
    public static Configuration config = init();

    private static Configuration init() {
        Configuration config = new Configuration(Configuration.VERSION_2_3_22);
        config.setOutputEncoding("utf-8");
        config.setTagSyntax(Configuration.AUTO_DETECT_TAG_SYNTAX);
        config.setTemplateExceptionHandler(TemplateExceptionHandler.HTML_DEBUG_HANDLER);
        config.setAutoEscapingPolicy(Configuration.ENABLE_IF_SUPPORTED_AUTO_ESCAPING_POLICY);
        config.setOutputFormat(HTMLOutputFormat.INSTANCE);
        config.addAutoInclude("/bamboo/views/layout.ftl");
        BeansWrapper beansWrapper = new BeansWrapper(Configuration.VERSION_2_3_26);
        beansWrapper.setExposeFields(true);
        config.setObjectWrapper(beansWrapper);
        config.setClassForTemplateLoading(Freemarker.class, "/");
        return config;
    }

    public static String render(Request request, String view, Object... keysAndValues) {
        if ("json".equals(request.queryParams("format"))) {
            return gson.toJson(keysAndValues);
        }

        Map<String, Object> model = new HashMap<>();
        model.put("request", request);
        for (int i = 0; i < keysAndValues.length; i += 2) {
            model.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        StringWriter sw = new StringWriter();
        try {
            config.getTemplate(view).process(model, sw);
        } catch (TemplateException | IOException e) {
            throw new RuntimeException(view, e);
        }
        return sw.toString();
    }
}
