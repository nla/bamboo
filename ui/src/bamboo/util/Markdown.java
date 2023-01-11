package bamboo.util;

import com.vladsch.flexmark.ext.gfm.strikethrough.StrikethroughExtension;
import com.vladsch.flexmark.ext.tables.TablesExtension;
import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.data.MutableDataSet;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Safelist;
import org.jsoup.select.Elements;

import java.net.URI;
import java.util.Arrays;


public class Markdown {

    public Markdown() {}

    private static final Safelist HTML_SAFELIST = Safelist.relaxed().preserveRelativeLinks(true);

    public static String render(String markdown, String baseUri) {
        if (markdown == null || markdown.isEmpty()) {
            return null;
        }

        MutableDataSet options = new MutableDataSet();

        options.set(Parser.EXTENSIONS, Arrays.asList(TablesExtension.create(), StrikethroughExtension.create()));
        options.set(HtmlRenderer.SOFT_BREAK, "<br />\n");

        Parser parser = Parser.builder(options).build();
        HtmlRenderer renderer = HtmlRenderer.builder(options).build();

        var document = parser.parse(markdown);
        String html = renderer.render(document);

        Document dirty = Jsoup.parseBodyFragment(html, baseUri.toString());
        Cleaner cleaner = new Cleaner(HTML_SAFELIST);
        Document clean = cleaner.clean(dirty);
        rewriteFragmentLinks(clean, baseUri);
        return clean.body().html();
    }

    private static void rewriteFragmentLinks(Document doc, String baseUri) {
        Elements links = doc.select("a");
        for (Element link : links) {
            String href = link.attr("href");
            if (href != null && href.startsWith("#")) {
                link.attr("href", baseUri + href);
            }
        }
    }
}
