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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CdxDateRange {
  private static final Logger log = LoggerFactory.getLogger(CdxDateRange.class);

  public Date start = null;
  public Date end = null;

  public boolean contains(Date date) {
    if (date == null) return false;

    return (start == null || start.compareTo(date) < 0) &&
            (end == null || end.compareTo(date) > 0);
  }

  public boolean hasData() {
    return start != null || end != null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    CdxDateRange that = (CdxDateRange) o;

    if (start != null ? !start.equals(that.start) : that.start != null) return false;
    return end != null ? end.equals(that.end) : that.end == null;

  }

  @Override
  public int hashCode() {
    int result = start != null ? start.hashCode() : 0;
    result = 31 * result + (end != null ? end.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
    return "CdxDateRange:[" + (start == null ? "NULL" : sdf.format(start)) + " TO "
            + (start == null ? "NULL" : sdf.format(end)) + "]";
  }
}
