Architecture
============

As at November 2015.

![architecture diagram](https://cdn.rawgit.com/nla/bamboo/90ef3d6/doc/architecture.svg)

### Wayback

The Internet Archive and IIPC's web archive replay tool.  Given a set of
CDX-indexed WARC files, can replay archived pages from WARC files rewriting
links on the fly.

### tinycdxserver ("CDX" index)

Maintains the full list of captured URLs, timestamps and which WARC file they
belong to.  Allows range queries like find all URLs starting with the given
prefix.

Stored in a [RocksDB index](http://rocksdb.org/) and compressed to reduce the
size from around 4 TB to 700 GB.

### AGWA Solr index

* Solr version: 4 (cloud mode)
* Size: ~900 GB
* Sharding: 4 shards, single replica, all shards in single JVM
* Schema:
  * id (url + timestamp)
  * url
  * timestamp
  * site domain name
  * media type
  * full text content
  * content length
  * http status code (unused)
  * SHA-1 content digest (unused)
  * boiled text (unused)

The indexer is a single 300 line class that just loops over all records in WARC
file, extracts text from HTML (using Boilerpipe) and PDFs (using iText) and posts
the records to Solr.

### AGWA beta delivery application

A fairly simple Grails application that wraps Wayback in an iframe and queries
Wayback's API to render calendar pages and the timeslider.  It also wraps the
AGWA Solr index and renders search results.


Strawman future Tarkine and Trove integration
---------------------------------------------

This is one possible way things could work, the technical details and exactly
who is responsible for what would need to be worked out by the different teams.

![future diagram](https://cdn.rawgit.com/nla/bamboo/052b3b3/doc/future-strawman.svg)

Tarkine and Trove essentially replace the AGWA beta application.  The Trove
team either takes ownership of the AGWA Solr index or replaces it with their own.

The current Solr index only includes the government archive, not the complete
web archive.  The full archive is roughly 15 times larger, so a complete index
might require a substantial amount of new hardware.

### Bamboo records API

Insulates Tarkine, Trove and Wayback from ingest and data storage concerns. They
don't have to care about crawls, filesystems or WARC files.

Answers these queries:

* Which records (snapshots/timestamps) are available for the given URL?
* Which records have been newly added or updated since my last indexing run? (Updates to existing records would mainly just be deletes due to access control takedowns.)
* What are all the records? (for full reindexing)
* Retrieve full WARC record.

### PANDAS integration / migration

By late 2016 to early 2017 I'd like to have all the PANDORA content in Bamboo.  PANDAS
is not going away in the near term, but it will become just another source of WARC
files for Bamboo.

There are two main challenges with doing so:

* PANDAS content is already link-rewritten. Normally WARC files should be the original unmodified content.
* PANDAS access restrictions are based around titles and instances rather than URL patterns.

### Questions

* Does Trove want to handle text extraction (parsing HTML, PDFs, Word docs etc) or 
  would it be easier if Bamboo provides an API with text already extracted?
* Do we need to support some of the PANDORA title metadata (subjects, titles)?
* Do we need to support browse like the current PANDORA website?
