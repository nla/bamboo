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
    }

    @Test
    public void textExtractTika() throws IOException, TextExtractionException {
        Document doc = new Document();
        try (InputStream stream = getClass().getResourceAsStream("example.odt")) {
            TextExtractor.extractTika(stream, doc);
        }
        assertEquals("Visible title\n" +
                "This is an example.\n", doc.getText());
        assertEquals("Metadata title", doc.getTitle());
    }
}