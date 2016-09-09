package bamboo.util;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;
import org.pegdown.PegDownProcessor;

import java.net.URI;

import static org.pegdown.Extensions.*;

public class Markdown {

    public Markdown() {}

    private static final int PEGDOWN_OPTIONS = AUTOLINKS | TABLES | FENCED_CODE_BLOCKS | HARDWRAPS | STRIKETHROUGH |
            SMARTYPANTS;
    private static final Whitelist HTML_WHITELIST = Whitelist.relaxed().preserveRelativeLinks(true);

    public static String render(String markdown, URI baseUri) {
        if (markdown == null || markdown.isEmpty()) {
            return null;
        }
        String html = new PegDownProcessor(PEGDOWN_OPTIONS).markdownToHtml(markdown);
        Document dirty = Jsoup.parseBodyFragment(html, baseUri.toString());
        Cleaner cleaner = new Cleaner(HTML_WHITELIST);
        Document clean = cleaner.clean(dirty);
        rewriteFragmentLinks(clean, baseUri);
        return clean.body().html();
    }

    private static void rewriteFragmentLinks(Document doc, URI baseUri) {
        Elements links = doc.select("a");
        for (Element link : links) {
            String href = link.attr("href");
            if (href != null && href.startsWith("#")) {
                link.attr("href", baseUri.toString() + href);
            }
        }
    }
}
