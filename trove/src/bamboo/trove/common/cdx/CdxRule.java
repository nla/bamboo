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

import bamboo.trove.common.DocumentStatus;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

/**
 * This (and other classes in this package) are essentially mirroring the classes in the tinycdxserver/outbackcdx with
 * some adaptations of class structure to meet indexing requirements and to avoid the plumbing code the cdx server has
 * because it is also the management interface and/or data store.
 * <p>
 * It would be good to extract these into a common code module and separate the match/access logic from everything else.
 */
@SuppressWarnings("unused")
public class CdxRule {
  private static final Logger log = LoggerFactory.getLogger(CdxRule.class);

  private Long id;
  private Long policyId;
  private DocumentStatus indexerPolicy;
  private List<String> urlPatterns;
  private CdxDateRange captured;
  private CdxDateRange accessed;
  @JsonDeserialize(using = CdxRule.PeriodDeserializer.class)
  @JsonSerialize(using = CdxRule.PeriodSerializer.class)
  private Period period;
  private String privateComment;
  private String publicMessage;
  private boolean enabled;
  private Date created;
  private Date modified;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  long getPolicyId() {
    return policyId;
  }

  public void setPolicyId(long policyId) {
    this.policyId = policyId;
  }

  public DocumentStatus getIndexerPolicy() {
    return indexerPolicy;
  }

  void setIndexerPolicy(DocumentStatus indexerPolicy) {
    this.indexerPolicy = indexerPolicy;
  }

  public List<String> getUrlPatterns() {
    return urlPatterns;
  }

  public void setUrlPatterns(List<String> urlPatterns) {
    this.urlPatterns = urlPatterns;
  }

  public CdxDateRange getCaptured() {
    return captured;
  }

  public void setCaptured(CdxDateRange captured) {
    this.captured = captured;
  }

  public CdxDateRange getAccessed() {
    return accessed;
  }

  public void setAccessed(CdxDateRange accessed) {
    this.accessed = accessed;
  }

  public Period getPeriod() {
    return period;
  }

  public void setPeriod(Period period) {
    this.period = period;
  }

  public String getPrivateComment() {
    return privateComment;
  }

  public void setPrivateComment(String privateComment) {
    this.privateComment = privateComment;
  }

  public String getPublicMessage() {
    return publicMessage;
  }

  public void setPublicMessage(String publicMessage) {
    this.publicMessage = publicMessage;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public Date getCreated() {
    return created;
  }

  public void setCreated(Date created) {
    this.created = created;
  }

  public Date getModified() {
    return modified;
  }

  public void setModified(Date modified) {
    this.modified = modified;
  }

  boolean matchesDates(Date captureTime, Date accessTime) {
    return (captured == null || captured.contains(captureTime)) &&
            (accessed == null || accessed.contains(accessTime)) &&
            (period == null || isWithinPeriod(captureTime, accessTime));
  }

  private boolean isWithinPeriod(Date captureTime, Date accessTime) {
    // do the period calculation in the local timezone so that 'years' periods work
    LocalDateTime localCaptureTime = LocalDateTime.ofInstant(captureTime.toInstant(), ZoneId.systemDefault());
    LocalDateTime localAccessTime = LocalDateTime.ofInstant(accessTime.toInstant(), ZoneId.systemDefault());
    return localAccessTime.isBefore(localCaptureTime.plus(period));
  }

  boolean hasDateComponent() {
    if (captured != null && captured.hasData()) {
      return true;
    }
    if (accessed != null && accessed.hasData()) {
      return true;
    }
    return (period != null && !period.isZero());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    CdxRule cdxRule = (CdxRule) o;

    if (enabled != cdxRule.enabled) return false;
    if (id != null ? !id.equals(cdxRule.id) : cdxRule.id != null) return false;
    if (indexerPolicy != cdxRule.indexerPolicy) return false;
    if (urlPatterns != null ? !urlPatterns.equals(cdxRule.urlPatterns) : cdxRule.urlPatterns != null) return false;
    if (captured != null ? !captured.equals(cdxRule.captured) : cdxRule.captured != null) return false;
    if (accessed != null ? !accessed.equals(cdxRule.accessed) : cdxRule.accessed != null) return false;
    if (period != null ? !period.equals(cdxRule.period) : cdxRule.period != null) return false;
    if (created != null ? !created.equals(cdxRule.created) : cdxRule.created != null) return false;
    return modified != null ? modified.equals(cdxRule.modified) : cdxRule.modified == null;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (indexerPolicy != null ? indexerPolicy.hashCode() : 0);
    result = 31 * result + (urlPatterns != null ? urlPatterns.hashCode() : 0);
    result = 31 * result + (captured != null ? captured.hashCode() : 0);
    result = 31 * result + (accessed != null ? accessed.hashCode() : 0);
    result = 31 * result + (period != null ? period.hashCode() : 0);
    result = 31 * result + (enabled ? 1 : 0);
    result = 31 * result + (created != null ? created.hashCode() : 0);
    result = 31 * result + (modified != null ? modified.hashCode() : 0);
    return result;
  }

  Stream<String> ssurtPrefixes() {
    return urlPatterns.stream().map(CdxAccessControl::toSsurtPrefix);
  }

  @Override
  public String toString() {
    return "AccessRule{" +
            "id=" + id +
            ", policyId=" + policyId +
            ", urlPatterns=" + urlPatterns +
            ", captured=" + captured +
            ", accessed=" + accessed +
            ", period=" + period +
            ", privateComment='" + privateComment + '\'' +
            ", publicMessage='" + publicMessage + '\'' +
            ", enabled=" + enabled +
            ", created=" + created +
            ", modified=" + modified +
            '}';
  }

  // JDK8 Periods are de/serialised strangely by Jackson and don't match how CDX does them
  public static class PeriodDeserializer extends StdDeserializer<Period> {
    private PeriodDeserializer() {
      this(null);
    }
    PeriodDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public Period deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      return Period.of(get(node, "years"), get(node, "months"), get(node, "days"));
    }

    private int get(JsonNode node, String key) {
      if (node.has(key)) {
        return (Integer) node.get(key).numberValue();
      }
      return 0;
    }
  }

  // We need a custom serializer to make sure we store the objects in MySQL the same way they were sent to us
  public static class PeriodSerializer extends StdSerializer<Period> {
    private PeriodSerializer() {
      this(null);
    }

    PeriodSerializer(Class<Period> t) {
      super(t);
    }

    @Override
    public void serialize(Period value, JsonGenerator gen, SerializerProvider provider) throws IOException {
      gen.writeStartObject();
      gen.writeNumberField("years", value.getYears());
      gen.writeNumberField("months", value.getMonths());
      gen.writeNumberField("days", value.getDays());
      gen.writeEndObject();
    }
  }
}
