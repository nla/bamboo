package bamboo.crawl;

import java.util.Date;

public class RecordStats {
    private long records;
    private long recordBytes;
    private Date startTime = null;
    private Date endTime = null;

    public void update(long recordLength, Date time) {
        records += 1;
        recordBytes += recordLength;

        if (startTime == null || time.before(startTime)) {
            startTime = time;
        }

        if (endTime == null || time.after(endTime)) {
            endTime = time;
        }
    }

    public long getRecords() {
        return records;
    }

    public long getRecordBytes() {
        return recordBytes;
    }

    public Date getStartTime() {
        return startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    @Override
    public String toString() {
        return "RecordStats{" +
                "records=" + records +
                ", recordBytes=" + recordBytes +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}
