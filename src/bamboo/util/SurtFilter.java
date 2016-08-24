package bamboo.util;

import java.util.ArrayList;
import java.util.List;

public class SurtFilter {
    final List<Rule> rules = new ArrayList<>();

    public SurtFilter(String ruleList) {
        for (String line : ruleList.split("\n")) {
            line = line.trim();
            if (!line.isEmpty()) {
                rules.add(new Rule(line));
            }
        }
    }

    public boolean accepts(String surt) {
        boolean decision = true;
        for (Rule rule : rules) {
            if (surt.startsWith(rule.prefix)) {
                decision = rule.policy;
            }
        }
        return decision;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SurtFilter that = (SurtFilter) o;

        if (rules != null ? !rules.equals(that.rules) : that.rules != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return rules != null ? rules.hashCode() : 0;
    }

    private static class Rule {
        final String prefix;
        final boolean policy;

        Rule (String text) {
            if (text.startsWith("+")) {
                policy = true;
            } else if (text.startsWith("-")) {
                policy = false;
            } else {
                throw new RuntimeException("Filter policies must begin with + or -");
            }
            prefix = text.substring(1);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Rule rule = (Rule) o;

            if (policy != rule.policy) return false;
            if (prefix != null ? !prefix.equals(rule.prefix) : rule.prefix != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = prefix != null ? prefix.hashCode() : 0;
            result = 31 * result + (policy ? 1 : 0);
            return result;
        }
    }

}
