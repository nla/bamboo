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

public enum SolrEnum {
  // Keeping them in alphabetical order just
  // to help readability as the schema grows
  AU_GOV("auGov"),
  DATE("date"),
  DECADE("decade"),
  DELIVERABLE("deliverable"),
  DISCOVERABLE("discoverable"),
  FILENAME("filename"),
  FULL_TEXT("fulltext"),
  HOST("host"),
  HOST_REVERSED("hostReversed"),
  ID("id"),
  LAST_INDEXED("lastIndexed"),
  METADATA("metadata"),
  PROTOCOL("protocol"),
  RULE("ruleId"),
  SEARCH_CATEGORY("searchCategory"),
  SITE("site"),
  TEXT_ERROR("textError"),
  TITLE("title"),
  URL("url"),  
  URL_TOKENIZED("urlTokenized"),
  YEAR("year");
  
  private String value;
  SolrEnum(String value) {
    this.value = value;
  }
  public String toString() {
    return value;
  }
}
