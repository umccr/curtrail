# Sample CLI ICA

This is a simple Python project that references the `curtrail` libary
locally (by local file path). 

It shows an example of how to use the `curtrail` library
for ICA bills and is configured
to run against local test data.

## Example run

```bash
uv run ica_compute_by_project.py --days "March"
```

generates

```
┌───────────────────────┬───────────────────────────┬───────────────────────┬─────────────────┬───────────┬────────────┐
│ USAGE_CONTEXT         ┆ META_referencePipeline    ┆ META_referenceVersion ┆ success_percent ┆ mean_cost ┆ total_cost │
│ ---                   ┆ ---                       ┆ ---                   ┆ ---             ┆ ---       ┆ ---        │
│ str                   ┆ str                       ┆ str                   ┆ f64             ┆ f64       ┆ f64        │
╞═══════════════════════╪═══════════════════════════╪═══════════════════════╪═════════════════╪═══════════╪════════════╡
│ BSSH icav2-sequencing ┆ null                      ┆ null                  ┆ 1.0             ┆ 70.0      ┆ 280.0      │
│ development           ┆ dragen-wgts-dna           ┆ 4-4-4                 ┆ 1.0             ┆ 48.22     ┆ 48.22      │
│ development           ┆ oncoanalyser-wgts-dna     ┆ 2-3-0                 ┆ 0.5             ┆ 10.055    ┆ 20.11      │
│ development           ┆ sash                      ┆ 0-7-0                 ┆ 0.0             ┆ 8.41      ┆ 16.82      │
│ development           ┆ sash                      ┆ 0-6-4-rc              ┆ 0.0             ┆ 2.276667  ┆ 6.83       │
│ production            ┆ dragen-wgts-dna           ┆ 4-4-4                 ┆ 0.833333        ┆ 67.420417 ┆ 1618.09    │
│ production            ┆ oncoanalyser-wgts-dna     ┆ 2-2-0                 ┆ 0.904762        ┆ 23.41619  ┆ 491.74     │
│ production            ┆ dragen-wgts-dna           ┆ 4-4-6                 ┆ 0.8             ┆ 56.748    ┆ 283.74     │
│ production            ┆ dragen-wgts-rna           ┆ 4-4-4                 ┆ 1.0             ┆ 7.306154  ┆ 94.98      │
│ production            ┆ oncoanalyser-wgts-dna-rna ┆ 2-2-0                 ┆ 0.818182        ┆ 8.228182  ┆ 90.51      │
│ production            ┆ oncoanalyser-wgts-rna     ┆ 2-2-0                 ┆ 1.0             ┆ 4.498462  ┆ 58.48      │
│ production            ┆ sash                      ┆ 0-6-3                 ┆ 0.857143        ┆ 2.750476  ┆ 57.76      │
│ production            ┆ arriba-wgts-rna           ┆ 2-5-0                 ┆ 1.0             ┆ 0.806154  ┆ 10.48      │
│ production            ┆ rnasum                    ┆ 2-0-0                 ┆ 1.0             ┆ 0.472143  ┆ 6.61       │
│ production            ┆ bclconvert-interop-qc     ┆ 1-5-0                 ┆ 1.0             ┆ 0.165     ┆ 0.99       │
│ production            ┆ dragen-tso500-ctdna       ┆ 2-6-0                 ┆ 1.0             ┆ 0.0       ┆ 0.0        │
│ project-meso-research ┆ dragen-wgts-dna           ┆ 4-4-4                 ┆ 1.0             ┆ 51.63619  ┆ 2168.72    │
│ project-meso-research ┆ oncoanalyser-wgts-dna     ┆ 2-2-0                 ┆ 0.976744        ┆ 17.097674 ┆ 735.2      │
│ project-meso-research ┆ sash                      ┆ 0-6-3                 ┆ 1.0             ┆ 2.19      ┆ 2.19       │
│ staging               ┆ dragen-tso500-ctdna       ┆ 2-6-0                 ┆ 1.0             ┆ 0.0       ┆ 0.0        │
└───────────────────────┴───────────────────────────┴───────────────────────┴─────────────────┴───────────┴────────────┘
```
