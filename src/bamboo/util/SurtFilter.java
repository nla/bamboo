package bamboo.util;

import org.archive.url.SURT;

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
    }

}
