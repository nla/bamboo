package bamboo.virus;

import java.time.Instant;

public record VirusFinding(long warcId, long recordOffset, String warcRecordId, String targetUri,
                           Instant captureTime, String mediaType, String payloadDigest, String signature) {
}

