# Probe runs

Single unlabelled runs, not protocol measurements. They were taken while
developing or validating an experiment and are kept because each was the first
evidence of something:

- `q1-streaming-1gb.txt` — the first run showing Phase 1 completing at a 1 GB
  dimension where the published implementation fails (§13).
- `smoke-par.txt`, `smoke-seq.txt`, `smoke-sql.txt` — the first check that the
  three scenario-unpivot implementations agree per scenario (§19).
- `q1-joinless.txt`, `q1-smj.txt`, `q2-joinless.txt` — instrumentation probes.

None of these carries a median, a discarded warm-up run or an interleaved
comparison. Every number in the reports comes from the subdirectories beside
this one, not from here.
