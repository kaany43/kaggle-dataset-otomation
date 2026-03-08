# Kaggle Dataset Automation for European Football Player Stats

This project collects player profile and season statistics from SofaScore leagues, builds CSV outputs, generates Kaggle metadata, and publishes updates to a Kaggle dataset.

## What the automation does

1. Resolves the active season for each configured league.
2. Fetches squad lists from league teams.
3. Collects player profiles and season-level stats.
4. Writes:
   - `all_player_profiles.csv`
   - `all_player_stats.csv`
   - `dataset-metadata.json` (when Kaggle identity is configured)
5. Uploads to Kaggle as:
   - A new version (`kaggle datasets version`) by default
   - A new dataset (`kaggle datasets create`) when requested

## Project structure

```text
.
|-- auto.py                   # Main pipeline and CLI
|-- collector.py              # SofaScore data collection layer
|-- writer.py                 # CSV and Kaggle metadata generation
|-- uploader.py               # Kaggle CLI upload wrapper
|-- requirements.txt
|-- .env.example
`-- .github/workflows/update.yml
```

## Requirements

- Python 3.11+
- Kaggle API credentials

Install dependencies:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

## Environment variables

Create a `.env` file from `.env.example`:

```bash
cp .env.example .env
```

Set the following values:

- `KAGGLE_USERNAME`
- `KAGGLE_KEY`
- `KAGGLE_DATASET_SLUG`

## Local usage

Run the pipeline and allow upload if Kaggle credentials are set:

```bash
python auto.py
```

Skip upload and only generate local files:

```bash
python auto.py --no-upload
```

Create a new Kaggle dataset (first upload):

```bash
python auto.py --new-dataset
```

Set custom version notes:

```bash
python auto.py --version-notes "Weekly refresh"
```

Run a faster smoke test with only one league:

```bash
python auto.py --no-upload --max-leagues 1 --league-delay 0
```

## GitHub Actions setup

The workflow file is:

- `.github/workflows/update.yml`

It runs daily at `04:00 UTC` and can also be started manually with optional inputs.

Required repository secrets:

- `KAGGLE_USERNAME`
- `KAGGLE_KEY`
- `KAGGLE_DATASET_SLUG`

### First publish vs updates

- First publish: run workflow manually with `new_dataset=true`
- Daily updates: keep `new_dataset=false` (default)

## Output files

Generated under `data/top_10/`:

- `all_player_profiles.csv`
- `all_player_stats.csv`
- `dataset-metadata.json` (only when Kaggle username and slug are available)

These files are ignored by git and are intended to be build artifacts.
