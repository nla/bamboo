package bamboo.trove.common;

/**
 * Sets the threshold of content that will be put into the Solr index. The list is ordered
 * by size (for maintenance... has no technical meaning) and each lower entry covers all
 * entries above it as well.
 */
public enum ContentThreshold {
  NONE,                 // Index nothing
  METADATA_ONLY,        // Index only simple metadata such as title and url
  UNIQUE_TERMS_ONLY,    // Start to include a set of distinctive terms found in the document
  DOCUMENT_START_ONLY,  // Index all terms in the document up to a defined size limit
  FULL_TEXT             // Index everything
}
