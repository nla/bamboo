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
package bamboo.trove.services;

import java.util.Arrays;
import java.util.List;

import bamboo.task.Document;
import bamboo.trove.common.ContentThreshold;
import org.springframework.stereotype.Service;

@Service
public class QualityControlService {
  public static final List<String> TEXT_CONTENT_TYPES = Arrays.asList("text/html", "application/pdf");

  public ContentThreshold filterDocument(Document document) {
    // Status code - Unless we actually harvested the content, don't index it
    if (document.getStatusCode() != 200) {
      return ContentThreshold.NONE;
    }

    // Content Type
    if (!TEXT_CONTENT_TYPES.contains(document.getContentType())) {
      return ContentThreshold.METADATA_ONLY;
    }

    // More rules will likely be added here
    return ContentThreshold.FULL_TEXT;
  }
}