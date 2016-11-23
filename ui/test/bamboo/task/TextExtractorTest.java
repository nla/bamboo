package bamboo.task;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;

import com.lowagie.text.pdf.PdfReader;
import org.junit.Test;

public class TextExtractorTest {

    @Test
    public void testExtractPdfContent() throws IOException, TextExtractionException {
        Document doc = new Document();
        TextExtractor.extractPdfContent(new PdfReader(getClass().getResource("example.pdf")), doc);
        assertEquals("The title of a test PDF\n" +
                "This is a test PDF file. It was created by LibreOffice. ", doc.getText());
        assertEquals("The title field in the metadata", doc.getTitle());
    }

    @Test
    public void textExtractPdfBox() throws IOException, TextExtractionException {
        Document doc = new Document();
        try (InputStream stream = getClass().getResourceAsStream("example.pdf")) {
            TextExtractor.extractPdfBox(stream, doc);
        }
        assertEquals("The title of a test PDF" + System.lineSeparator()  +
                "This is a test PDF file. It was created by LibreOffice." + System.lineSeparator(), doc.getText());
        assertEquals("The title field in the metadata", doc.getTitle());
        assertEquals("The keywords field in the metadata", doc.getKeywords());
        assertEquals("The subject field in the metadata", doc.getCoverage());
    }

    @Test
    public void textExtractTika() throws IOException, TextExtractionException {
        Document doc = new Document();
        try (InputStream stream = getClass().getResourceAsStream("example.odt")) {
            TextExtractor.extractTika(stream, doc);
        }
        assertEquals("Visible title" + System.lineSeparator()  +
                "This is an example." + System.lineSeparator(), doc.getText());
        assertEquals("Metadata title", doc.getTitle());
    }

    @Test
    public void textExtractBadTitle() throws IOException, TextExtractionException {
        Document doc = new Document();
        try (InputStream stream = getClass().getResourceAsStream("badtitle.html")) {
            TextExtractor.extractTika(stream, doc);
        }
        assertEquals("Ministerial Decision and Recommendations: New South Wales Ocean Trawl Fishery", doc.getTitle());
        assertEquals("Test", doc.getText().trim());
        assertEquals("this is a description", doc.getDescription());
        assertEquals("this is keywords", doc.getKeywords());
    }
}