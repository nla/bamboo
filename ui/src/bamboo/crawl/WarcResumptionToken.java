package bamboo.crawl;

import java.time.Instant;

public class WarcResumptionToken {
    public static final WarcResumptionToken MIN_VALUE = new WarcResumptionToken(Instant.EPOCH, Long.MIN_VALUE);

    public final Instant time;
    public final long id;
    public final long urlCount;

    public WarcResumptionToken(Instant time, long id, long urlCount) {
        this.time = time;
        this.id = id;
        this.urlCount = urlCount;
    }

    public WarcResumptionToken(Instant time, long id) {
        this(time, id, -1);
    }

    static WarcResumptionToken parse(String s) {
        String parts[] = s.split(",");
        return new WarcResumptionToken(Instant.parse(parts[0]), Long.parseLong(parts[1]));
    }

    public String toString() {
        return time.toString() + "," + Long.toString(id);
    }
}
