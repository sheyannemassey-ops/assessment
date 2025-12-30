# assessment

This workspace contains a small Python tool to fetch patient data from the DemoMed Healthcare API,
compute per-patient risk scores, identify alert lists (high-risk, fever, data-quality issues),
and optionally submit the results back to the assessment API.

Usage:

- Install dependencies:

```bash
python -m pip install -r requirements.txt
```

- Run a dry-run (no submission, safe to run multiple times):

```bash
python assessment.py
```

- Run and submit results to the assessment endpoint (consumes a submission attempt):

```bash
python assessment.py --submit
```

The script reads the API key from the `ASSESSMENT_API_KEY` environment variable if set,
otherwise it uses a default key embedded for this session. Use `--help` for more options.
# assessment