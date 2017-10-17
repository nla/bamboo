package bamboo.task;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class HeadingContentHandler extends DefaultHandler {
    private static Pattern WHITESPACE_RE = Pattern.compile("\\s+");
    private List<String> headings = new ArrayList();
    private StringBuilder buffer = new StringBuilder();
    private int depth = 0;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (depth > 0 || localName.equals("h1")) {
            depth++;
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (depth == 1) { // we reached the end of the outer-most h1 tag
            headings.add(WHITESPACE_RE.matcher(buffer).replaceAll(" ").trim());
            buffer.setLength(0);
        }
        if (depth > 0) {
            depth--;
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (depth > 0) {
            buffer.append(ch, start, length);
        }
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        characters(ch, start, length);
    }

    public List<String> getHeadings() {
        return headings;
    }
}
