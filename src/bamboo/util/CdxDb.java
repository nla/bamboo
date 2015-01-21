package bamboo.util;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CdxDb {
    public static class CdxEntry implements Serializable {
        public final String originalUrl;
        public final long timestamp;
        public final String mime;
        public final int status;
        public final String digest;
        public final String warc;
        public final long offset;

        public CdxEntry(String[] f) {
            timestamp = Long.parseLong(f[1]);
            originalUrl = f[2];
            mime = f[3];
            status = Integer.parseInt(f[4]);
            digest = f[5];
            offset = Long.parseLong(f[9]);
            warc = f[10];
        }
    }

    public static void main(String args[]) throws IOException {
        DB db = DBMaker.newFileDB(new File(args[1]))
                .closeOnJvmShutdown()
                .compressionEnable()
                .make();
        BTreeMap<String, String> map = db.getTreeMap("cdx");

        try (BufferedReader rdr = Files.newBufferedReader(Paths.get(args[0]))) {
            for (;;) {
                String line = rdr.readLine();
                if (line == null) break;
                if (line.startsWith(" ")) continue;
                String[] fields = line.split(" ", 2);
                map.put(fields[0], fields[1]);

                /*
                try {
                    CdxEntry entry = new CdxEntry(fields);
                    map.put(fields[0], entry);
                } catch (NumberFormatException e) {
                    // skip it
                }
                */
            }
        }

        db.commit();
        db.close();
    }
}
