package bamboo.task;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.commons.httpclient.util.URIUtil;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class CdxRequestEncoder {
    private static final int MAX_LENGTH = 4096;

    public String encode(String method, byte[] body, String contentType) {

        return null;
    }

    public static String encodeJson(String json) throws IOException {
        StringBuilder output = new StringBuilder();
        JsonParser parser = new JsonFactory().createParser(json);
        Map<String,Long> serials = new HashMap<>();
        Deque<String> nameStack = new ArrayDeque<>();
        String name = null;
        while (parser.nextToken() != null && output.length() < MAX_LENGTH) {
            switch (parser.currentToken()) {
                case FIELD_NAME:
                    name = parser.getCurrentName();
                    break;
                case VALUE_FALSE:
                case VALUE_TRUE:
                case VALUE_NUMBER_FLOAT:
                case VALUE_STRING:
                case VALUE_NUMBER_INT:
                case VALUE_NULL:
                    if (name != null) {
                        long serial = serials.compute(name, (key, value) -> value == null ? 1 : value + 1);
                        String key = name;
                        if (serial > 1) {
                            key += "." + serial + "_";
                        }
                        output.append('&');
                        output.append(URIUtil.encodeWithinQuery(key));
                        output.append('=');
                        String value;
                        switch (parser.currentToken()) {
                            case VALUE_NULL:
                                value = "None"; // using Python names for pywb compatibility
                                break;
                            case VALUE_FALSE:
                                value = "False";
                                break;
                            case VALUE_TRUE:
                                value = "True";
                                break;
                            case VALUE_NUMBER_INT:
                                value = String.valueOf(parser.getLongValue());
                                break;
                            case VALUE_NUMBER_FLOAT:
                                value = String.valueOf(parser.getDoubleValue());
                                break;
                            default:
                                value = URIUtil.encodeWithinQuery(parser.getValueAsString());
                        }
                        output.append(value);
                    }
                    break;
                case START_OBJECT:
                    if (name != null) {
                        nameStack.push(name);
                    }
                    break;
                case END_OBJECT:
                    name = nameStack.isEmpty() ? null : nameStack.pop();
                    break;
                case START_ARRAY:
                case END_ARRAY:
                    break;
                default:
                    throw new IllegalStateException("Unexpected: " + parser.currentToken());
            }
        }
        return output.toString();
    }
    
}
