# Crawl Report Plan

## Purpose

This document is a working plan for a crawl reporting tool for broad crawls such as `.gov.au`, `.edu.au`, and wider `.au` crawls.

The report is intended primarily for the technical team, with enough clarity that management can occasionally use it to understand crawl outcomes, operational problems, and progress over time.

## Decision

The reporting system should be developed as a separate tool, not as Bamboo-native core functionality.

Bamboo may later integrate with the tool by:

- linking to generated reports
- storing generated outputs as crawl artifacts
- optionally triggering report generation

But the report generator itself should not depend on Bamboo’s database model or crawl lifecycle.

## Why Separate Is The Better Default

The main reasons are:

- the tool should be able to run on a crawl before it has been ingested into Bamboo
- the tool should be able to run on a crawl that is still in progress and produce partial output
- reporting logic is likely to evolve quickly and should not require frequent changes to a core production system
- other institutions may want to use the tool without adopting Bamboo
- the reporting engine’s natural inputs are WARC files and crawl artifacts, not Bamboo records

This means Bamboo is best treated as one consumer of report outputs rather than the home of the reporting engine.

## Goals

- Show whether a crawl is meeting collection goals around breadth, depth, and diversity.
- Surface both recurring and newly emerging crawl problems.
- Support comparisons across previous crawls so trends are visible.
- Provide human-readable output that can mix generated metrics with human-written observations.
- Keep the output portable enough that it can be reused outside Bamboo.

## Core Design Principle

The reporting tool should be input-driven, not system-driven.

In other words, the unit of work should be something like:

- a crawl directory
- a set of WARC files
- a Heritrix job directory
- a crawl artifact bundle

not:

- a Bamboo crawl id

That keeps the tool usable across:

- pre-ingest validation
- in-progress crawl monitoring
- post-ingest reporting
- standalone historical analysis
- non-Bamboo environments

## Primary Inputs

The reporting tool should accept some combination of:

- WARC files
- crawl directories
- Heritrix job directories
- extracted crawl artifacts
- Bamboo-exported artifact bundles where available
- optional metadata provided by the operator
  - crawl name
  - crawl scope
  - series or program name
  - notes

## Outputs

The tool should produce both machine-readable and human-readable outputs.

### Machine-readable outputs

- summary metrics
- issue summaries
- per-host aggregates
- per-domain aggregates
- seed outcome summaries
- comparison data

### Human-readable outputs

- Markdown report
- HTML report

The machine-readable outputs are important because they let us:

- regenerate reports without re-parsing everything in some cases
- compare crawls later
- integrate with Bamboo or other systems
- export the data for other institutions or workflows

## Recommended Output Layout

Per crawl or report run, generate a directory like:

- `summary.json`
- `issues.json`
- `hosts.json.gz`
- `domains.json.gz`
- `seeds.json.gz`
- `report.md`
- `report.html`
- `metadata.json`

For partial or in-progress reports, include a clear status flag in metadata so consumers know the report is incomplete.

## Storage Strategy

The reporting tool itself should avoid requiring a heavy central database.

Preferred first approach:

- store outputs as ordinary files
- keep large aggregates compressed
- make the report reproducible from raw inputs

This keeps the tool simple, portable, and cheap to operate.

If we later need cross-crawl analytics at scale, we can add a small index or separate analytics store, but that should not be required for the first version.

## Suggested Architecture

Use a layered structure:

1. Input adapters
   - understand WARC paths, Heritrix directories, crawl bundles, and optional Bamboo exports
2. Extractors
   - compute metrics and detect issues from inputs
3. Intermediate data model
   - a stable, tool-owned report schema
4. Renderers
   - Markdown and HTML report generation
5. Integrations
   - Bamboo integration
   - future CLI or batch workflow integration

This keeps Bamboo-specific concerns at the edge rather than in the middle of the design.

## Bamboo’s Role

Bamboo should be treated as an optional integration target.

Possible Bamboo integration points later:

- upload generated report outputs as crawl artifacts
- display summary metrics on the crawl page
- link to the latest generated report
- trigger the external reporting tool for an archived crawl

Important constraint:

- Bamboo should not be required in order to generate a report

## Functional Scope By Phase

## Phase 1: Standalone single-crawl report

Goal: generate a useful report from one crawl without any Bamboo dependency.

Deliverables:

- CLI entry point
- input handling for a Heritrix crawl directory and/or WARC set
- summary JSON output
- Markdown report
- optional HTML render

Metrics to target first:

- crawl scope and identifying metadata
- start time, end time, duration if recoverable
- pages visited
- resources captured
- total bytes
- hosts visited
- domains visited
- average crawl depth if recoverable
- seed outcome totals if recoverable

Issue categories to target first:

- bot blocking / Cloudflare-like blocking
- failed seeds
- reject-rule or crawler-trap evidence

### Phase 1 principle

Prefer trustworthy, clearly sourced metrics over ambitious but uncertain ones.

## Phase 2: Comparisons and trends

Goal: compare one crawl report to prior report outputs.

Deliverables:

- comparison mode between two report datasets
- trend summaries across a set of report outputs
- issue trend sections in generated reports

Important point:

- this phase should compare report datasets, not require live access to Bamboo

That keeps the comparison model reusable and portable.

## Phase 3: Partial and in-progress reporting

Goal: support running the tool on a crawl that is still underway.

Deliverables:

- partial report mode
- explicit completeness markers
- metrics that distinguish final values from current observed values

Examples:

- current hosts reached
- current domains reached
- observed blocking indicators so far
- seeds attempted vs not yet attempted if that is derivable

## Phase 4: Bamboo integration

Goal: make the external tool convenient to use from Bamboo without tightly coupling them.

Possible deliverables:

- Bamboo UI link to uploaded report artifacts
- manual “generate report” action for a crawl
- optional task that shells out to the report tool
- headline summary metrics shown on the crawl page

This phase should stay thin. Bamboo should consume outputs rather than own report internals.

## Extraction Strategy

The tool should have modular extractors so each source can improve independently.

Suggested extractor categories:

- WARC aggregate extractor
  - totals, bytes, hosts, domains, MIME types, duplicate indicators, depth proxies
- artifact parser
  - logs, configs, crawl reports, reject rules
- seed outcome extractor
  - successful, failed, blocked, unresolved
- issue detector
  - classify blocking, crawler traps, and operational problems
- comparison builder
  - compare one report dataset with others
- renderer
  - produce Markdown and HTML from the intermediate model

## Intermediate Data Model

The tool should define and own a stable report schema.

That schema should include:

- crawl metadata
- report generation metadata
- completeness state
- headline metrics
- issue counts and evidence samples
- per-host aggregates
- per-domain aggregates
- optional comparison sections

This schema should be versioned so outputs remain interpretable as the tool evolves.

## Report Content Direction

The initial report should probably include:

- scope of crawl and seed context if available
- start time, end time, duration
- pages visited, resources captured, bytes
- host and domain coverage
- average depth or another depth summary if reliable
- seed outcomes
- blocking and trap observations
- major differences from a previous comparable crawl if comparison input is available

The report should also leave room for manually written observations.

For example:

- operator notes
- interpretation of anomalies
- follow-up actions

These notes can initially live in an optional input file rather than in Bamboo.

## Data Management Principles

- Do not require a database for the first version.
- Do not store per-URL or per-record derived data unless there is a very strong reason.
- Prefer compact aggregates over raw event retention.
- Prefer reproducible outputs over opaque internal state.
- Keep large outputs compressed.

## Technology Direction

This document does not lock in an implementation language yet, but the tool should ideally be:

- easy to run from the command line
- easy to package for batch execution
- comfortable for iterative parsing and report generation work

That points toward a standalone CLI-oriented tool rather than a web application.

## Open Questions

- Which exact Heritrix artifacts are consistently present across representative crawls?
- Which requested metrics are directly recoverable from WARCs versus only from logs or reports?
- How reliable are duplicate or novel-content metrics from currently available inputs?
- What is the best comparison unit:
  - prior crawl in a named program
  - prior crawl in a supplied report set
  - arbitrary selected baseline
- Do we want manual notes embedded from sidecar Markdown or YAML files?
- Should Bamboo integration eventually be pull-based, push-based, or both?

## Proposed Next Steps

1. Inspect one or two representative crawl directories or crawl bundles and inventory the exact available inputs.
2. Create a metric extractability matrix:
   - requested metric
   - likely source
   - confidence level
   - notes
3. Define a first-version report schema owned by the tool.
4. Decide the initial tool shape:
   - CLI only
   - CLI plus library
5. Build a thin proof of concept that reads one crawl and emits:
   - `summary.json`
   - `issues.json`
   - `report.md`

## Initial Recommendation

Build the crawl reporting capability as a standalone, input-driven reporting tool with portable outputs and optional Bamboo integration.

That gives us:

- freedom to iterate
- support for pre-ingest and in-progress crawls
- easier reuse by other institutions
- lower risk to Bamboo as a core production system
