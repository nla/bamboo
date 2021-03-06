package bamboo.crawl;

import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public interface NamedStream {
    String name();
    long length();
    InputStream openStream() throws IOException;

    static NamedStream of(MultipartFile multipartFile) {
        return new NamedStream() {
            @Override
            public String name() {
                return multipartFile.getOriginalFilename();
            }

            @Override
            public long length() {
                return multipartFile.getSize();
            }

            @Override
            public InputStream openStream() throws IOException {
                return multipartFile.getInputStream();
            }
        };
    }

    static List<NamedStream> of(MultipartFile[] warcFiles) {
        var streams = new ArrayList<NamedStream>();
        for (MultipartFile warcFile : warcFiles) {
            if (warcFile.isEmpty()) continue;
            streams.add(NamedStream.of(warcFile));
        }
        return streams;
    }
}
