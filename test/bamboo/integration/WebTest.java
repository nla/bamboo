package bamboo.integration;

import bamboo.web.Webapp;
import com.gargoylesoftware.htmlunit.*;
import com.gargoylesoftware.htmlunit.util.NameValuePair;
import droute.*;
import net.sourceforge.jwebunit.htmlunit.HtmlUnitTestingEngineImpl;
import net.sourceforge.jwebunit.junit.WebTester;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class WebTest {

    public static class HtmlUnitRequest implements Request {
        WebRequest request;
        MultiMap params = new MultiMap();
        MultiMap urlParams = new MultiMap();
        MultiMap queryParams;

        HtmlUnitRequest(WebRequest request) {
            this.request = request;
            queryParams = decodeParameters(uri().getRawQuery());
            params.putAll(queryParams);
        }

        @Override
        public Object raw() {
            return request;
        }

        @Override
        public String method() {
            return request.getHttpMethod().name();
        }

        @Override
        public URI uri() {
            try {
                return request.getUrl().toURI();
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String contextPath() {
            return "/";
        }

        @Override
        public Map<String, String> headers() {
            return request.getAdditionalHeaders();
        }

        @Override
        public String path() {
            return "/" + contextUri().relativize(uri()).toString();
        }

        @Override
        public URI contextUri() {
            return uri().resolve("/");
        }

        @Override
        public MultiMap params() {
            return decodeParameters(uri().getRawQuery());
        }

        @Override
        public MultiMap urlParams() {
            return urlParams;
        }

        @Override
        public MultiMap queryParams() {
            return queryParams;
        }

        @Override
        public MultiMap formParams() {
            return null;
        }

        @Override
        public String postBody() {
            return null;
        }

        @Override
        public void setState(Object o) {

        }

        @Override
        public <T> T state(Class<T> aClass) {
            return null;
        }

        /**
         * Decode percent encoded <code>String</code> values.
         *
         * From NanoHttpd
         *
         * @param str the percent encoded <code>String</code>
         * @return expanded form of the input, for example "foo%20bar" becomes "foo bar"
         */
        protected String decodePercent(String str) {
            String decoded = null;
            try {
                decoded = URLDecoder.decode(str, "UTF8");
            } catch (UnsupportedEncodingException ignored) {
            }
            return decoded;
        }

        /**
         * Decode parameters from a URL, handing the case where a single parameter name might have been
         * supplied several times, by return lists of values.  In general these lists will contain a single
         * element.
         *
         * From NanoHttpd.
         *
         * @param queryString a query string pulled from the URL.
         * @return a map of <code>String</code> (parameter name) to <code>List&lt;String&gt;</code> (a list of the values supplied).
         */
        protected MultiMap decodeParameters(String queryString) {
            MultiMap parms = new MultiMap();
            if (queryString != null) {
                StringTokenizer st = new StringTokenizer(queryString, "&");
                while (st.hasMoreTokens()) {
                    String e = st.nextToken();
                    int sep = e.indexOf('=');
                    String propertyName = (sep >= 0) ? decodePercent(e.substring(0, sep)).trim() : decodePercent(e).trim();
                    String propertyValue = (sep >= 0) ? decodePercent(e.substring(sep + 1)) : null;
                    if (propertyValue != null) {
                        parms.put(propertyName, propertyValue);
                    }
                }
            }
            return parms;
        }
    }

    public static class DrouteWebConnection implements WebConnection {

        final Handler handler;

        public DrouteWebConnection(Handler handler) {
            this.handler = handler;
        }

        @Override
        public WebResponse getResponse(WebRequest webRequest) throws IOException {
            Request request = new HtmlUnitRequest(webRequest);
            Response response = handler.handle(request);
            Streamable streamer = streamify(response.body());
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            streamer.writeTo(body);
            List<NameValuePair> headers = new ArrayList<>();
            for (Map.Entry<String, String> entry : response.headers().entrySet()) {
                headers.add(new NameValuePair(entry.getKey(), entry.getValue()));
            }
            WebResponseData data = new WebResponseData(body.toByteArray(), response.status(), "Something", headers);
            return new WebResponse(data, webRequest, 0);
        }
    }

    private static Streamable streamify(Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof Streamable) {
            return (Streamable) obj;
        } else if (obj instanceof InputStream) {
            return (out) -> {
                InputStream in = (InputStream)obj;
                byte buf[] = new byte[16384];
                int len;
                while ((len = in.read(buf)) >= 0) {
                    out.write(buf, 0, len);
                }
            };
        } else if (obj instanceof String) {
            return (out) -> out.write(((String)obj).getBytes(StandardCharsets.UTF_8));
        } else {
            throw new IllegalArgumentException("unable to handle body of type " + obj.getClass());
        }
    }

    @Test
    @Ignore("not ready yet")
    public void test() {
        WebTester t = new WebTester();

        t.setDialog(new HtmlUnitTestingEngineImpl() {
            @Override
            protected WebClient createWebClient() {
                WebClient client = super.createWebClient();
                client.setWebConnection(new DrouteWebConnection(new Webapp()));
                return client;
            }
        });

        t.setBaseUrl("http://unittest.localhost:8080/");

        t.beginAt("/");
        t.clickLink("navCrawlSeries");


    }
}
