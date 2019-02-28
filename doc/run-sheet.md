# Bamboo Run Sheet

## Recovering the Trove Solr indexer from runaway access control rule changes

If an access control rule is overlay broad, for example "\*" or some variant like "\*.au" it may force the Solr indexer to
process large parts of the index. This will cause indexing to get stuck in the rules change phase. Stopping the indexer
and changing the rules in OutbackCDX will not solve the problem as the indexer stores its own copy of the rules and
will enter "recovery mode" and will simply resume processing where it left off.

1. Stop the indexer.
2. Remove the offending pattern from the JSON in  the indexer's copy of the rule in MySQL table
  index_persistence_web_archives_restrictions_rules(make sure you select the row with the correct ruleSetId).
3. Delete the offending rule from OutbackCDX.
4. Recreate the offending rule with a new rule id.
5. Start the indexer.

This will cause the indexer to:

1. Enter recovery mode and try processing the problem rule again.
2. But since the broad pattern is gone it should finish quickly and move on to processing the rest of the rules.
3. After finishing recovery it'll process the "next night's" rules which will include the deletion of the bad rule and creation
   of the replacement. This will cause all the incorrectly restricted documents to be reprocessed and corrected.
