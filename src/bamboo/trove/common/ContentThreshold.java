/**
 * Copyright 2016 National Library of Australia
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
