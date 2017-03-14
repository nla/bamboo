/*
 * Copyright 2017 National Library of Australia
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
package bamboo.trove.common.cdx;

import java.util.Date;

public class CdxDateRange {
  public Date start;
  public Date end;

  public boolean contains(Date date) {
    if (date == null) return false;

    return (start == null || start.compareTo(date) < 0) &&
            (end == null || end.compareTo(date) > 0);
  }

  public boolean hasData() {
    return start != null || end != null;
  }
}
