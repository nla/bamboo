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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.netpreserve.urlcanon.ByteString;
import org.netpreserve.urlcanon.Canonicalizer;
import org.netpreserve.urlcanon.ParsedUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import com.googlecode.concurrenttrees.radixinverted.ConcurrentInvertedRadixTree;
import com.googlecode.concurrenttrees.radixinverted.InvertedRadixTree;

import bamboo.trove.common.DocumentStatus;

public class CdxAccessControl {
  private static Logger log = LoggerFactory.getLogger(CdxAccessControl.class);

  private static final String CDX_DELIVERABLE = "public (delivery)";
  private static final String CDX_DISCOVERABLE = "public (discovery)";
  private static final Pattern WWW_PREFIX = Pattern.compile("^www\\d*\\.");
  private static final DocumentStatus DEFAULT_POLICY = DocumentStatus.ACCEPTED;
  private static final CdxRule DEFAULT_RULE;
  static {
    DEFAULT_RULE = new CdxRule();
    DEFAULT_RULE.setIndexerPolicy(DEFAULT_POLICY);
    DEFAULT_RULE.setId(-1);
  }

  private final Map<Long, CdxRule> rules;
  private final RulesBySsurt rulesBySurt;

  /**
   * This constructor should be used when loading policies and rules together from the CDX server.
   *
   * It will internally translate the policies to be useful for the indexer and then update the rules
   * to be self contained and ready for storage. Afterwards only the Rules need to be persisted,
   * not the policies.
   *
   * @param policies The CDX Access Policy objects pre-translation.
   * @param rules The CDX Rules without translated policy references.
   */
  public CdxAccessControl(List<CdxAccessPolicy> policies, List<CdxRule> rules) {
    // Must load policies first
    Map<Long, DocumentStatus> translatedPolicies = loadPolicies(policies);
    this.rules = loadRules(rules, translatedPolicies);

    rulesBySurt = new RulesBySsurt(rules);

    if (policies.isEmpty()) {
      throw new IllegalArgumentException("No policies loaded");
    }
  }

  /**
   * This constructor should be used when loading rule sets out of the database. No real processing is required
   * because we already did that when we first received the data.
   *
   * @param rules The CDX Rules from the database, WITH translated policy references.
   */
  public CdxAccessControl(List<CdxRule> rules) {
    this.rules = new TreeMap<>();
    for (CdxRule rule : rules) {
      this.rules.put(rule.getId(), rule);
    }
    rulesBySurt = new RulesBySsurt(rules);
  }

  private Map<Long, DocumentStatus> loadPolicies(List<CdxAccessPolicy> input) {
    Map<Long, DocumentStatus> map = new TreeMap<>();
    for (CdxAccessPolicy policy : input) {
      map.put(policy.getId(), translateCdxPolicy(policy));
    }
    return map;
  }

  /**
   * We translate the CDX Policies for two reasons:
   *  1) we need to make the indexer immune to IDs and access changing inside CDX which don't have any impact on
   *     the restrictions in Solr. eg. we don't care that a policy has ID#2, we care what restriction it enacts.
   *     A rule could be policy #2 today, but be changed to #4 tomorrow, but still have the same impact on the Solr
   *     index. We don't want to re-index it unless a 'real' change has happened.
   *  2) for convenience we don't want to handle all of the various restriction states that the CDX server handles,
   *     we just filter it down to being those which are relevant to the indexer.
   *
   * @param cdxPolicy The CDX Policy to translate
   * @return An indexer DocumentStatus that this policy represents.
   */
  private DocumentStatus translateCdxPolicy(CdxAccessPolicy cdxPolicy) {
    if (cdxPolicy.getAccessPoints().contains(CDX_DELIVERABLE)) {
      if (cdxPolicy.getAccessPoints().contains(CDX_DISCOVERABLE)) {
        return DocumentStatus.ACCEPTED;
      } else {
        return DocumentStatus.RESTRICTED_FOR_DISCOVERY;
      }
    }
    if (cdxPolicy.getAccessPoints().contains(CDX_DISCOVERABLE)) {
      return DocumentStatus.RESTRICTED_FOR_DELIVERY;
    }
    return DocumentStatus.RESTRICTED_FOR_BOTH;
  }

  private Map<Long, CdxRule> loadRules(List<CdxRule> input, Map<Long, DocumentStatus> policies) {
    Map<Long, CdxRule> map = new TreeMap<>();
    for (CdxRule rule : input) {
      if (!policies.containsKey(rule.getPolicyId())) {
        throw new IllegalArgumentException("No matching policy for ID '" + rule.getPolicyId()
                + "'. Found in Rule '" + rule.getId() + "'");
      }
      rule.setIndexerPolicy(policies.get(rule.getPolicyId()));
      map.put(rule.getId(), rule);
    }
    return map;
  }

  public Map<Long, CdxRule> getRules() {
    return rules;
  }

  /**
   * For a url convert into a search url that should match with the way it is normalised for delivery.
   * @param url
   * @return
   */
  public static String getSearchUrl(String url){
  	ParsedUrl parsed = ParsedUrl.parseUrl(url);
  	if(parsed.getScheme().isEmpty()){
  		// default to http as this is needed to force the host to be detected
  		parsed = ParsedUrl.parseUrl("http://"+url);
  	}
  	Canonicalizer.WHATWG.canonicalize(parsed);
  	parsed.setPath(parsed.getPath().asciiLowerCase());
  	parsed.setHost(parsed.getHost().replaceAll(WWW_PREFIX, ""));
  	String ret = parsed.getHost().toString() + parsed.getPath().toString();
  	return ret;
  }


  /**
   * Find all rules that may apply to the given URL.
   */
  private List<CdxRule> rulesForUrl(String url) {
    return rulesBySurt.prefixing(canonSsurt(url));
  }

  private static String canonSsurt(String url) {
    ParsedUrl parsed = ParsedUrl.parseUrl(url);
    Canonicalizer.WHATWG.canonicalize(parsed);
    parsed.setPath(parsed.getPath().asciiLowerCase());
    parsed.setFragment(ByteString.EMPTY);
    parsed.setHashSign(ByteString.EMPTY);
    parsed.setHost(parsed.getHost().replaceAll(WWW_PREFIX, ""));
    if (parsed.getScheme().toString().equals("https")) {
      parsed.setScheme(new ByteString("http"));
    }
    return parsed.ssurt().toString();
  }

  private static void reverseDomain(String host, StringBuilder out) {
    int i = host.lastIndexOf('.');
    int j = host.length();
    while (i != -1) {
      out.append(host, i + 1, j);
      out.append(',');
      j = i;
      i = host.lastIndexOf('.', i - 1);
    }
    out.append(host, 0, j);
    out.append(',');
  }

  /**
   * Converts an exact URL, a URL containing pywb-style "*" wildcards to a SSURT prefix.
   * SSURTs are passed through unaltered. Exact matches are suffixed with a space.
   */
  static String toSsurtPrefix(String pattern) {
    if (pattern.startsWith("*.")) {
      if (pattern.contains("/")) {
        throw new IllegalArgumentException("can't use a domain wildcard with a path");
      }
      StringBuilder out = new StringBuilder();
      reverseDomain(pattern.substring(2), out);
      return out.toString().toLowerCase();

    } else if (pattern.endsWith("*")) {
      return canonSsurt(pattern.substring(0, pattern.length() - 1));

    } else {
      return canonSsurt(pattern.substring(0, pattern.length())) + " ";
    }
  }

  public CdxRule checkAccess(String url, Date captureTime, Date accessTime) {
    return checkDateAccess(captureTime, accessTime, rulesForUrl(url));
  }

  private CdxRule checkDateAccess(Date captureTime, Date accessTime, List<CdxRule> rules) {
    // Find the most specific rule
    CdxRule match = null;
    for (CdxRule rule : rules) {
      if (rule.matchesDates(captureTime, accessTime)) {
        match = rule;
      }
    }
    // We don't return inside the loop because we won't the 'lowest' (most specific) rule to match
    if (match != null) {
      return match;
    }
    return DEFAULT_RULE;
  }

  public long size() {
    return rules.size();
  }

  public boolean isEmpty() {
    return rules.isEmpty();
  }

  public RulesDiff checkForRulesChanges(CdxAccessControl newRules) {
    RulesDiff diff = new RulesDiff();

    log.info("Comparing THIS ({} rules) to THAT ({} rules)", getRules().size(), newRules.getRules().size());
    int adds = 0, changes = 0, deletes = 0;

    // Find each rule in the new rule set
    for (CdxRule rule : rules.values()) {
      if (newRules.rules.containsKey(rule.getId())) {
        CdxRule newRule = newRules.rules.get(rule.getId());
        if (!newRule.equals(rule)) {
          // They don't match anymore
          changes++;
          diff.addChangedRule(rule, newRule);
        }

      // Not found in the new rule set... register as a delete
      } else {
        deletes++;
        diff.addDeletedRule(rule);
      }
    }

    // Now scan for new rules
    for (CdxRule rule : newRules.rules.values()) {
      if (!rules.containsKey(rule.getId())) {
        adds++;
        diff.addNewRule(rule);
      }
    }

    log.info("NEW     rules = {}", adds);
    log.info("DELETE  rules = {}", deletes);
    log.info("CHANGED rules = {}", changes);

    return diff;
  }

  /**
   * Return a list of rules from the larger rule set which have a date based component to them.
   * Disabled rules are not returned.
   *
   * @return A list of all rules which are both enabled and have date based components
   */
  public List<CdxRule> getDateBasedRules() {
    return rules.values().stream()
            .filter(rule -> rule.isEnabled() && rule.hasDateComponent())
            .collect(Collectors.toList());
  }

  /**
   * A secondary index for looking up access control URLs which prefix a
   * given SURT.
   * <p>
   * As the radix tree library can't handle an empty keys we prefix every key
   * by " " to allow for a default rule.
   */
  private static class RulesBySsurt {
    private final InvertedRadixTree<List<CdxRule>> tree;

    RulesBySsurt(Collection<CdxRule> rules) {
      tree = new ConcurrentInvertedRadixTree<>(new DefaultCharArrayNodeFactory());
      rules.forEach(this::put);
    }

    /**
     * Add an AccessRule to the radix tree. The rule will be added multiple times,
     * once for each SURT prefix.
     */
    void put(CdxRule rule) {
      rule.ssurtPrefixes().forEach(ssurtPrefix -> {
        String key = " " + ssurtPrefix;
        List<CdxRule> list = tree.getValueForExactKey(key);
        if (list == null) {
          list = Collections.synchronizedList(new ArrayList<>());
          tree.put(key, list);
        }
        list.add(rule);
      });
    }

    private List<CdxRule> prefixing(String ssurt) {
      return flatten(tree.getValuesForKeysPrefixing(" " + ssurt + " "));
    }

    static List<CdxRule> flatten(Iterable<List<CdxRule>> listsOfRules) {
      List<CdxRule> result = new ArrayList<>();
      listsOfRules.forEach(result::addAll);
      return result;
    }
  }
}
