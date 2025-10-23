# Bamboo Changelog

## 0.7.1

### Dependency upgrades

* **bootswatch-paper**: 3.3.4+1 → 3.3.7
* **chosen**: removed
* **commons-codec**: 1.18.0 → 1.19.0
* **commons-compress**: 1.27.1 → 1.28.0
* **commons-io**: 2.18.0 → 2.20.0
* **gson**: 2.12.1 → 2.13.2
* **guava**: 33.4.0-jre → 33.5.0-jre
* **jackson**: 2.15.2 → 2.20
* **jdbi3-sqlobject**: 3.48.0 → 3.49.6
* **jquery**: 2.1.3 → 3.7.1
* **jwarc**: 0.31.1 → 0.32.0
* **jwebunit-htmlunit-plugin**: removed
* **maven-compiler-plugin**: 3.11.0 → 3.14.1
* **maven-shade-plugin**: 3.2.2 → 3.6.1
* **maven-source-plugin**: 3.3.0 → 3.3.1
* **methanol**: 1.8.3 → 1.8.4
* **nanojson**: 1.9 → 1.10
* **spring-boot**: 3.4.3 → 3.5.6
* **springdoc-openapi-starter-webmvc-ui**: 2.1.0 → 2.8.13
* **tika**: 3.1.0 → 3.2.3
* **webarchive-commons**: 1.3.0 → 3.0.0

## 0.6.1

### Bug fixes
- **Skip incomplete response records:** Browsertrix records with no headers or body are now skipped during indexing.
  This works around an issue where incompletely recorded responses break replay.