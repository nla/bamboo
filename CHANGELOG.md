# Bamboo Changelog

## 0.7.1

### Bug fixes
- **Skip incomplete response records:** Browsertrix records with no headers or body are now skipped during indexing.
  This works around an issue where incompletely recorded responses break replay.