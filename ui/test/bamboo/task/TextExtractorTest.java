package bamboo.task;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TextExtractorTest {

    private static TextExtractor textExtractor;

    @BeforeClass
    public static void setup() {
        textExtractor = new TextExtractor();
    }

    @AfterClass
    public static void teardown() {
        textExtractor.close();
    }

    @Test
    public void textExtractPdfBox() throws IOException, TextExtractionException {
        Document doc = new Document();
        try (InputStream stream = getClass().getResourceAsStream("example.pdf")) {
            textExtractor.extractTika(stream, doc,  URI.create("http://example.net/subdir/example.pdf"));
        }
        assertEquals("The title of a test PDF This is a test PDF file. It was created by LibreOffice. The title of a test PDF", doc.getText());
        assertEquals("The title field in the metadata", doc.getTitle());
        assertEquals("The keywords field in the metadata", doc.getKeywords());
        assertEquals("The subject field in the metadata", doc.getCoverage());
    }

    @Test
    public void textExtractTika() throws IOException, TextExtractionException {
        Document doc = new Document();
        try (InputStream stream = getClass().getResourceAsStream("example.odt")) {
            textExtractor.extractTika(stream, doc, URI.create("http://example.net/subdir/example.odt"));
        }
        assertEquals("Visible title\n" +
                "This is an example.", doc.getText());
//        assertEquals("Metadata title", doc.getTitle()); // FIXME: doesn't seem to work with ForkParser
    }

    @Test
    public void textExtractBadTitle() throws IOException, TextExtractionException {
        Document doc = new Document();
        try (InputStream stream = getClass().getResourceAsStream("badtitle.html")) {
            textExtractor.extractTika(stream, doc, URI.create("http://example.net/subdir/badtitle.html"));
        }
        assertEquals("Ministerial Decision and Recommendations: New South Wales Ocean Trawl Fishery", doc.getTitle());
        assertEquals("Heading one!" +
                " Test\n" +
                "Link textHeading two!", doc.getText().trim());
        assertEquals("this is a description", doc.getDescription());
        assertEquals("this is keywords", doc.getKeywords());

        assertEquals("http://example.net/subdir/style.css", doc.getLinks().get(1).getUrl());
        assertEquals("site name", doc.getOgSiteName());
        assertEquals("og title", doc.getOgTitle());
        assertEquals(Arrays.asList("Heading one!", "Heading two!"), doc.getH1());
    }

    @Test
    public void testHackOffPandoraUrl() throws TextExtractionException {
        Document doc = new Document();
        TextExtractor.setUrls(doc, "http://pandora.nla.gov.au/pan/160553/20161116-1000/www.smh.com.au/money/super-and-funds/some-rare-good-financial-news-for-younger-people-20161109-gsm0lh.html");
        assertEquals("http://www.smh.com.au/money/super-and-funds/some-rare-good-financial-news-for-younger-people-20161109-gsm0lh.html", doc.getUrl());
        assertEquals("http://pandora.nla.gov.au/pan/160553/20161116-1000/www.smh.com.au/money/super-and-funds/some-rare-good-financial-news-for-younger-people-20161109-gsm0lh.html", doc.getDeliveryUrl());
    }
}
