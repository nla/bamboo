package bamboo.task;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.regex.Pattern;

public class HeadingContentHandler extends DefaultHandler {
    private static Pattern WHITESPACE_RE = Pattern.compile("\\s+");
    private StringBuilder text = new StringBuilder();
    private int depth = 0;
    private boolean sawAnyHeadings = false;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (depth > 0 || localName.equals("h1")) {
            depth++;
            sawAnyHeadings = true;
            if (text.length() != 0) {
                text.append(" ");
            }
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (depth > 0) {
            depth--;
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (depth > 0) {
            text.append(ch, start, length);
        }
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        if (depth > 0) {
            text.append(ch, start, length);
        }
    }

    public String getText() {
        if (sawAnyHeadings) {
            return WHITESPACE_RE.matcher(text).replaceAll(" ");
        } else {
            return null;
        }
    }
}
