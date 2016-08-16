package bamboo.trove.services;

import bamboo.task.Document;
import bamboo.trove.common.ContentThreshold;
import org.springframework.stereotype.Service;

@Service
public class QualityControlService {
  public ContentThreshold filterDocument(Document document) {
    // TODO
    return ContentThreshold.FULL_TEXT;
  }
}