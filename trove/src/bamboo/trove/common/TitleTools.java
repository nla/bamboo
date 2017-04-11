package bamboo.trove.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities primarily intended for use inside the TransformWorker... because of the threading model in that worker
 * there is no need for thread safety here. These utilities are NOT thread safe.
 *
 * These rules have grown somewhat ad-hoc in a rulset roughly termed 'SEO Malus' in discussion with the business area.
 * The underlying intent is to penalize the relevancy of pages with overly long titles, and additionally penalize
 * those titles that also have a high degree of duplication. eg. where 'apple' has been spammed a lot as a keyword.
 *
 */
public class TitleTools {
  private static final Logger log = LoggerFactory.getLogger(TitleTools.class);

  // Don't even look at very short titles where the duplication could be legitimate
  private static final int SEO_MINIMUM_WORD_COUNT = 5;
  // The 'forced' dupe height will bypass the dupe minimum (% based) that would normally
  // protect a natural language title. We force it to apply to absurdly long documents.
  // ie. If a single word appears over 1000 times in the title it will wreck tf-idf scores
  // when left unchecked
  private static final int SEO_FORCED_DUPE_HEIGHT = 1000;
  // The base malus is used to ensure there is always some impact of SEO spam.
  // It will be further modify by length multipliers.
  private static final float SEO_BASE_MALUS = 0.9F;
  // We use this to massage how much impact the width of histograms and dupes will have.
  // NOTE: Don't use 1.0 (it causes zeroes in the math);
  private static final float HISTOGRAM_WEIGHT = 0.9F;
  private static final float DUPE_WEIGHT = 0.95F;
  private static final float NO_MALUS = 1.0F;
  // You could try 'Punct' but we are going to be even harsher
  // and strip every non-letter/number/space (allowing for UTF-8)
  private static final Pattern REGEX_PUNCTUATION = Pattern.compile("[^\\p{L}\\p{Digit}\\s]");

  public static float seoMalus(String input) {
    // Then work out the distribution of words inside
    Histogram histogram = makeHistogram(input);
    // For short strings this is all we do
    if (histogram == null || histogram.size() <= SEO_MINIMUM_WORD_COUNT) {
      return NO_MALUS;
    }

    // We want 'wide' histograms to be less severe since they represent less overall duplication
    float widthSeverity = 1 - histogram.widthRatio();
    // We also factor in the worst instance of duplication. Duplication that occurs just because
    // of natural language sentences doesn't get hist as bad by this one.
    float worstDuplicate = histogram.height() > 1 ? histogram.heightRatio() : 0;
    // If there isn't enough duplication... don't consider it at all
    if (worstDuplicate < 0.1F && histogram.height() < SEO_FORCED_DUPE_HEIGHT) {
      worstDuplicate = 0.0F;
    }

    // We now know that the title is going to get hit by the SEO hammer.
    // The SEO malus will be an exaggeration of the length malus, weighted by the level of duplication present
    // and then further weighted by 'width severity'. So really long titles that spam a small number of words
    // are hit the hardest overall, but the absurdly long titles with just a small amount of duplication will
    // still be hit.
    float lengthMalus = lengthMalus(input);
    float rawSeoMalus = lengthMalus * SEO_BASE_MALUS;
    float widthWeighting = (1 - (widthSeverity * HISTOGRAM_WEIGHT));
    float duplicateWeighting = (1 - (worstDuplicate * DUPE_WEIGHT));

    // We are done
    return rawSeoMalus * widthWeighting * duplicateWeighting;
  }

  public static float lengthMalus(String input) {
  	if(input == null) return NO_MALUS;
  	
    int i = input.length();
    if (i < 100) return NO_MALUS;

    // Long titles get penalized
    if (i >= 100000) return 0.3F;
    if (i >= 10000) return 0.5F;
    if (i >= 2000) return 0.6F;
    if (i >= 1000) return 0.7F;
    if (i >= 500) return 0.8F;
    if (i >= 200) return 0.9F;
    // 100 <-> 200
    return 0.95F;
  }

  public static Histogram makeHistogram(String input) {
    if (input == null || "".equals(input)) {
      return null;
    }

    Histogram histogram = new Histogram();
    Matcher matcher = REGEX_PUNCTUATION.matcher(input.toLowerCase());
    String strippedInput = matcher.replaceAll("");
    String[] words = strippedInput.split("\\s");
    for (String word : words) {
      histogram.add(word.trim());
    }
    return histogram;
  }

  public static class Histogram {
    private Map<String, Integer> entries = new HashMap<>();
    private int height = 0;
    private int count = 0;

    public void add(String word) {
      if (word == null || "".equals(word)) {
        return;
      }
      count++;

      // Existing
      if (entries.containsKey(word)) {
        int newValue = entries.get(word) + 1;
        entries.put(word, newValue);
        if (newValue > height) {
          height = newValue;
        }

      // New
      } else {
        entries.put(word, 1);
        if (height == 0) {
          height = 1;
        }
      }
    }

    public int height() {
      return height;
    }

    public int width() {
      return entries.size();
    }

    public int size() {
      return count;
    }

    public float widthRatio() {
      return ((float) width()) / ((float) size());
    }

    public float heightRatio() {
      return ((float) height()) / ((float) size());
    }
  }
}
