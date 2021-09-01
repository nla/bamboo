package bamboo.task;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class CdxTest {

    @Test
    public void encodeJson() throws IOException {
        assertEquals("", encodeJson(""));
        assertEquals("&a=1&a.2_=2&a.3_=3", encodeJson("{\"a\":[1,2,3]}"));
        assertEquals("&b=2&a=3", encodeJson("{\"a\":[{\"b\":2},3]}"));
        assertEquals("&a=1&b=2&a.2_=3", encodeJson("[{\"a\":1},{\"b\":2, \"a\":3}]"));
        assertEquals("&a=True&a.2_=False&a.3_=None&a.4_=&a.5_=1.0&a.6_=-0.0&a.7_=0&a.8_=1.0",
                encodeJson("{\"a\":[true,false,null,\"\",1.0,-0.0,-0,1.00]}"));
    }

    private String encodeJson(String json) throws IOException {
        return Cdx.encodeJsonRequest(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    }

}