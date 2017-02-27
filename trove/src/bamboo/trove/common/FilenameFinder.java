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
package bamboo.trove.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class FilenameFinder {
  @SuppressWarnings("unused")
  private static Logger log = LoggerFactory.getLogger(FilenameFinder.class);

  public static String getFilename(String input) {
    try {
      URI uri = URI.create(input);
      String path = uri.getPath();
      if (path == null || "".equals(path)) {
        return null;
      }

      // Remove everything beyond /
      int i = path.lastIndexOf("/");
      if (i == -1) {
        return path;
      }

      // It might be empty now
      path = path.substring(i + 1);
      if ("".equals(path)) {
        return null;
      }
      return path;

    } catch (Exception ex) {
      // Warcs contain a number of crappy URLs that cannot be parsed by the jdk...
      // we don't care about these edge cases
      return null;
    }
  }
}
