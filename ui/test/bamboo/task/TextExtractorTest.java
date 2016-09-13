package bamboo.task;

import com.lowagie.text.pdf.PdfReader;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;

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
        assertEquals("The title of a test PDF\n" +
                "This is a test PDF file. It was created by LibreOffice.\n", doc.getText());
        assertEquals("The title field in the metadata", doc.getTitle());
    }
}