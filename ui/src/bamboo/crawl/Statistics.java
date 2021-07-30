package bamboo.crawl;

import bamboo.util.Units;

public class Statistics {
    private long files;
    private long size;
    private long records;

    public Statistics(long files, long totalSize, long totalRecords) {
        this.files = files;
        this.size = totalSize;
        this.records = totalRecords;
    }

    public long getFiles() {
        return files;
    }

    public long getSize() {
        return size;
    }

    public String getDisplaySize() {
        return Units.displaySize(getSize());
    }

    public long getRecords() {
        return records;
    }

    public Statistics plus(Statistics other) {
        return new Statistics(getFiles() + other.getFiles(),
                getSize() + other.getSize(),
                getRecords() + other.getRecords());
    }
}
