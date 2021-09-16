# Bamboo

Keeps track of our web archiving collection, manages indexing and calculates statistics.

## Status

Usable for basic collection management but some functionality is unfinished or unpolished.

## Configuration

### SQL Database

Bamboo stores a catalogue of crawls, statistics and indexing progress in a H2 or MySQL database which can be configured
via the following environment variables.

    BAMBOO_DB_URL=jdbc:h2:mem:bamboo
    BAMBOO_DB_USER=bamboo
    BAMBOO_DB_PASSWORD=bamboo

### Heritrix integration

The following environment variables tell Bamboo where to look for Heritrix jobs.

    HERITRIX_JOBS=/heritrix/jobs
    HERITRIX_URLS=https://127.0.0.1:8443

### PANDAS integration

Bamboo has optional, limited integration with the National Library of Australia's legacy PANDAS 3 collection management
system.  It can be enabled by building with `mvn -Ppandas` and setting the following environment variables:

    PANDAS_DB_URL=
    PANDAS_DB_USER=
    PANDAS_DB_PASSWORD=

### Watching directories for changed WARCs

Bamboo can be configured to watch a directory for newly created or updated WARCs that follow the *.warc.gz.open
convention used by Heritrix, warcprox and other tools.

    BAMBOO_WATCH=<crawl-id1>:<dir1>,<crawl-id2>:<dir2>,...

For example:

    BAMBOO_WATCH=42,/tmp/crawler/warcs

Bamboo will watch for newly created *.warc.gz.open files, index them as new records are added and then move the WARC
into crawl 42's archive directory when the file is renamed to *.warc.gz.

### Tuning

    CDX_INDEXER_THREADS=4

### OpenID Connect

If your CDX server requires a bearer token from the OpenID auth server set the following:

    OIDC_URL=https://kc.example.org/auth/realms/myrealm
    OIDC_CLIENT_ID=
    OIDC_CLIENT_SECRET=
