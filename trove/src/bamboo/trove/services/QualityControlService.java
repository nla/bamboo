/*
 * Copyright 2016-2017 National Library of Australia
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
package bamboo.trove.services;

import bamboo.task.Document;
import bamboo.trove.common.ContentThreshold;
import bamboo.trove.common.IndexerDocument;

import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Service
public class QualityControlService {
  public static final List<String> HTML_CONTENT_TYPES = Collections.singletonList("text/html");
  public static final List<String> PDF_CONTENT_TYPES = Collections.singletonList("application/pdf");
  public static final List<String> DOCUMENT_CONTENT_TYPES = Arrays.asList("application/rtf", "application/msword",
          "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
          "application/vnd.oasis.opendocument.text");
  public static final List<String> PRESENTATION_CONTENT_TYPES = Arrays.asList("application/vnd.ms-powerpoint",
          "application/vnd.openxmlformats-officedocument.presentationml.presentation",
          "application/vnd.oasis.opendocument.presentation");
  public static final List<String> SPREADSHEET_CONTENT_TYPES = Arrays.asList("application/vnd.ms-excel",
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/csv", "application/csv",
          "application/vnd.oasis.opendocument.spreadsheet");

  ContentThreshold filterDocument(IndexerDocument indexerDocument) {
  	Document document = indexerDocument.getBambooDocument();
    // Status code - Unless we actually harvested the content, don't index it
    if (document.getStatusCode() != 200) {
      return ContentThreshold.METADATA_ONLY;
    }

    // Content Type
    if (isSearchableContentType(document)) {
    	if(!isFullTextSite(indexerDocument)){
    		return ContentThreshold.DOCUMENT_START_ONLY;
    	}
      return ContentThreshold.FULL_TEXT;
    }

    // More rules will likely be added here
    return ContentThreshold.METADATA_ONLY;
  }

  private boolean isSearchableContentType(Document document) {
    return HTML_CONTENT_TYPES.contains(document.getContentType())
            || PDF_CONTENT_TYPES.contains(document.getContentType())
            || PRESENTATION_CONTENT_TYPES.contains(document.getContentType())
            || SPREADSHEET_CONTENT_TYPES.contains(document.getContentType())
            || DOCUMENT_CONTENT_TYPES.contains(document.getContentType());
  }
  
  /**
   * We need to reduce the size of the index so we don't store all the full text for some sites.  
   * @param document
   * @return
   */
  private boolean isFullTextSite(IndexerDocument document){
    return (document.getBambooDocument().getSite().endsWith(".gov.au") || document.isPandora());
  }
}
