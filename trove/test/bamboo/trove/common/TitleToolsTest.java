package bamboo.trove.common;

import bamboo.trove.services.CdxRestrictionServiceTest;
import bamboo.trove.workers.TransformWorker;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import static org.junit.Assert.assertEquals;

public class TitleToolsTest {
  // We don't really care that much about precision... but with such small floats there's
  // no reason rounding would be acceptable anyway
  private static final float VERY_PRECISE_FLOAT = 0.000000001F;
  // For SEO there is a lot of floating point multipliers. We just want to know we are correct to
  // a 10th of a percentile. It will make tweaking numbers inside the SEO code less onerous to transcribe out here.
  private static final float BALLPARK_FLOAT = 0.001F;

  private static String getResource(String resourceName) throws IOException {
    try (InputStream is = CdxRestrictionServiceTest.class.getResourceAsStream("/seo.junk/" + resourceName)) {
      StringBuilder sb = new StringBuilder();
      try (Reader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"))) {
        int c;
        while ((c = reader.read()) != -1) {
          sb.append((char) c);
        }
        return sb.toString();
      }
    }
  }

  @Test
  public void histogramTest() {
    // Simple
    assertHistogram("I am four words", 4, 1, 4);
    assertHistogram("I am\nfour words", 4, 1, 4);
    assertHistogram("I. am\nfo-ur ! words", 4, 1, 4);
    // Natural language with low duplication (and case) 'The' and 'the' will combine
    assertHistogram("The quick brown fox jumps over the lazy dog", 9, 2, 8);
    // Stupid duplication
    assertHistogram("The the THE THe tHE thE T-he the! t.he", 9, 9, 1);
  }

  @Test
  public void lengthMalusTest() {
    assertLength(10, 1.0F);
    assertLength(99, 1.0F);
    assertLength(100, 0.95F);
    assertLength(199, 0.95F);
    assertLength(200, 0.9F);
    assertLength(499, 0.9F);
    assertLength(500, 0.8F);
    assertLength(999, 0.8F);
    assertLength(1000, 0.7F);
    assertLength(1999, 0.7F);
    assertLength(2000, 0.6F);
    assertLength(9999, 0.6F);
    assertLength(10000, 0.5F);
    assertLength(100000, 0.3F);
  }

  @Test
  public void seoMalusTest() throws IOException {
    // Stupid long title. > 445k characters
    // https://smallbusiness.yahoo.com...
    assertSeoResource("836661.58478340.txt", 445249, 0.3F, 0.151F);

    // Small SEO spam... heavy 'apple' duplication
    // http://dailykitty.com...
    assertSeoResource("699448.216761488.txt", 727, 0.8F, 0.287F);

    // Long duplicate spam... 'istanbul'
    // http://www.oldistanbul.com/
    assertSeoResource("701270.784689232.txt", 53976, 0.5F, 0.183F);

    // Confirm safety of tiny titles... even with duplicates
    assertSeo("spam spam spam spam spam", 24, 1.0F, 1.0F);
    assertSeo("spam spam spam spam spam spam", 29, 1.0F, 0.026F);
  }

  @Test
  public void junkWhitespaceTest() throws IOException {
    String title = assertSeoResource("5843.74468343.txt", 508846, 0.3f, 0.230f);
    // Most of this title is crap
    String cleanTitle  = TransformWorker.removeExtraSpaces(title);
    System.out.println(cleanTitle);
    assertSeo(cleanTitle, 210, 0.9f, 0.690f);
  }

  private String assertSeoResource(String document, int length, float lengthMalus, float seoMalus) throws IOException {
    String title = getResource(document);
    assertSeo(title, length, lengthMalus, seoMalus);
    return title;
  }

  private void assertSeo(String title, int length, float lengthMalus, float seoMalus) throws IOException {
    assertEquals("Title was not expected length", length, title.length());
    assertEquals("Length malus was incorrect", lengthMalus, TitleTools.lengthMalus(title), VERY_PRECISE_FLOAT);
    assertEquals("SEO malus was incorrect", seoMalus, TitleTools.seoMalus(title), BALLPARK_FLOAT);
  }

  private void assertLength(int length, float malus) {
    StringBuilder sb = new StringBuilder();
    // We don't care about dupes or spaces when looking at raw length
    for (int i = 0; i < length; i++) {
      sb.append("a");
    }
    float lengthMalus = TitleTools.lengthMalus(sb.toString());
    assertEquals("Length malus was incorrect", malus, lengthMalus, VERY_PRECISE_FLOAT);
  }

  private void assertHistogram(String input, int size, int height, int width) {
    TitleTools.Histogram h = TitleTools.makeHistogram(input);
    assertEquals("Histogram size is incorrect",   size,   h.size());
    assertEquals("Histogram height is incorrect", height, h.height());
    assertEquals("Histogram width is incorrect",  width,  h.width());
    float heightRatio = ((float) height) / (float) size;
    float widthRatio = ((float) width) / (float) size;
    assertEquals("Histogram heightRatio is incorrect",  heightRatio,  h.heightRatio(), VERY_PRECISE_FLOAT);
    assertEquals("Histogram widthRatio is incorrect",   widthRatio,   h.widthRatio(),  VERY_PRECISE_FLOAT);
  }
}
