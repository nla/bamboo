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

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class RulesDiff {
  private TreeMap<Long, RulesWrapper> rules = new TreeMap<>();

  public void addChangedRule(CdxRule oldRule, CdxRule newRule) {
    rules.put(oldRule.getId(), new RulesWrapper(oldRule, newRule, Reason.CHANGED));
  }

  public void addNewRule(CdxRule rule) {
    rules.put(rule.getId(), new RulesWrapper(rule, Reason.NEW));
  }

  public void addDeletedRule(CdxRule rule) {
    rules.put(rule.getId(), new RulesWrapper(rule, Reason.DELETED));
  }

  public boolean hasWorkLeft() {
    return !rules.isEmpty();
  }

  public int size() {
    return rules.size();
  }

  public void filterRules(long ruleId) {
    Iterator<Map.Entry<Long, RulesWrapper>> it = rules.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Long, RulesWrapper> entry = it.next();
      if (entry.getKey() <= ruleId) {
        it.remove();
      }
    }
  }

  public RulesWrapper nextRule() {
    Long key = rules.firstKey();
    return rules.remove(key);
  }

  // We have this primarily because we want to process rules in ID order, so we are going to merge all the
  // changed rules into a single list... but we still want to know the context for the rule requiring action
  public static class RulesWrapper {
    public CdxRule rule;
    public CdxRule newRule;
    public Reason reason;

    RulesWrapper(CdxRule rule, Reason reason) {
      this.rule = rule;
      this.reason = reason;
      this.newRule = null;
    }

    RulesWrapper(CdxRule rule, CdxRule newRule, Reason reason) {
      this.rule = rule;
      this.reason = reason;
      this.newRule = newRule;
    }
  }

  public enum Reason {
    CHANGED, NEW, DELETED
  }
}
