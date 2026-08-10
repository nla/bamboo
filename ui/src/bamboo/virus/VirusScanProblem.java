package bamboo.virus;

public record VirusScanProblem(long warcId, long recordOffset, String type, String message) {
    public VirusScanProblem(long warcId, String type, String message) {
        this(warcId, -1, type, message);
    }
}

