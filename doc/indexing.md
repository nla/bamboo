Indexing
========

Inputs
------

* WARC files
* Access control rules
* Directory metadata

Types of updates
----------------

* New WARCs
* Access control changes
* Metadata changes
* Indexer changes (i.e. full reindex for a schema update)

Access control index updates
----------------------------

* For each rule updated since last run (found using version numbers):
** Find the affected documents by SURT prefix
*** Recalculate new ACL field value and send to Solr



Requirements
------------

* Access rules are (at least initially) populated from PANDAS.
* Access to portions of the web archive can be limited to groups (publice, onsite, staff, partner onsite etc).
* Access groups can be scoped by IP address ranges.
* Access groups can be scoped by username/password logins.
* Access rules can be scoped by URL or portions of URL (domain, directory, file etc).
* Access rules can be scoped by the date of collection and embargo period.
* Access rules can display a custom note to users who are denied access.
* Access rules can keep a internal note to staff editing them.
* Changes to access control rules take effect in near realtime.
* Access control rules apply both to browsing and search.

Solr: Query-Time Access Control
-------------------------------

Access control and directory metadata is kept out of the Solr index. It's instead applied as a post-filtering step
at query time.  As each search result is returned it's URL is looked up an access decision is made.

Advantages:
* Simplifies indexing
* Realtime

Disadvantages:
* Can't display total matching document count
* Can't query or rank based on metadata
* Potentially unbearably slow when search results return a lot of restricted content

Solr: Index-Time Access Control
-------------------------------

Each Solr document includes a field `access` specifying a materialized list of access groups (public, staff,
nlareadingroom etc) that are allowed to view it.

When a search is made the application automatically adds a search term for each of the access groups the user has access
to.
    (prime minister) AND (access:public OR access:nlareadingroom)

When an access rule is changed, the affected documents need to be discovered and their access field updated. In order
to implement this an

Advantages:
* Simplifies querying
* All counts etc work as expected

Disadvantages:
* Have to update indexes
* Updates aren't realtime

