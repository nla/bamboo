package bamboo.task;

import com.lowagie.text.pdf.PdfReader;
import org.junit.Test;

import java.io.IOException;

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

}