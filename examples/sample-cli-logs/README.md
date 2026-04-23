# Sample CLI Logs

This is a simple Python project that references the `curtrail` libary
locally (by local file path). 

It shows an example of how to use the `curtrail` library
for cloudtrails and is configured
to run against local test data.

## Example run

```bash
uv run s3_last_data_access.py --days "Mar" --region "ap-southeast-2" --bucket "primary"
```

generates

```
┌──────────────────┬───────────────────────────────────────┬─────────────────────────┬──────────────┬──────────────┬───────────────────┬──────────────────────────┐
│ bucket           ┆ key                                   ┆ lastAccess              ┆ eventName    ┆ identityType ┆ identityActor     ┆ identityRole             │
│ ---              ┆ ---                                   ┆ ---                     ┆ ---          ┆ ---          ┆ ---               ┆ ---                      │
│ str              ┆ str                                   ┆ datetime[ms, UTC]       ┆ str          ┆ str          ┆ str               ┆ str                      │
╞══════════════════╪═══════════════════════════════════════╪═════════════════════════╪══════════════╪══════════════╪═══════════════════╪══════════════════════════╡
│ uog-primary-data ┆ inputs/2026-03-13/sample-001.fastq.gz ┆ 2026-03-13 06:00:02 UTC ┆ GetObject    ┆ AssumedRole  ┆ s3-data-processor ┆ s3-processor-lambda-role │
│ uog-primary-data ┆ inputs/2026-03-13/sample-002.fastq.gz ┆ 2026-03-13 06:00:08 UTC ┆ GetObject    ┆ AssumedRole  ┆ s3-data-processor ┆ s3-processor-lambda-role │
│ uog-primary-data ┆ restricted/clinical/patient-007.bam   ┆ 2026-03-11 09:41:22 UTC ┆ GetObject    ┆ AssumedRole  ┆ pipeline-session  ┆ uog-pipeline-role        │
│ uog-primary-data ┆ samples/2026-03-09/SampleSheet.csv    ┆ 2026-03-10 08:22:15 UTC ┆ GetObject    ┆ AssumedRole  ┆ pipeline-session  ┆ uog-pipeline-role        │
│ uog-primary-data ┆ tmp/scratch-20260313.gz               ┆ 2026-03-14 15:44:19 UTC ┆ DeleteObject ┆ AssumedRole  ┆ pipeline-session  ┆ uog-pipeline-role        │
└──────────────────┴───────────────────────────────────────┴─────────────────────────┴──────────────┴──────────────┴───────────────────┴──────────────────────────┘
```
