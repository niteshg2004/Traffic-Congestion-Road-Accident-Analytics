# Dataset instructions

## Dataset: US Accidents (Moosavi et al.)
A countrywide car-accident dataset covering 49 US states (about 7.7 million records in the
March-2023 release; the exact size depends on the release - **check it yourself and quote
your own count in the report**).

**Do not put this file in the ZIP / Git repo** - it is several GB.

## Where to download
1. Kaggle (free account needed): https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents
2. Author's page (links to the same data): https://smoosavi.org/datasets/us_accidents
3. Click **Download** -> you get `archive.zip` -> extract it.
4. Copy the CSV (e.g. `US_Accidents_March23.csv`) into **`data/raw/`**.

Command-line alternative (needs a Kaggle API token in `~/.kaggle/kaggle.json`):
```
pip install kaggle
kaggle datasets download -d sobhanmoosavi/us-accidents -p data/raw --unzip
```
> Availability, size and file name may change over time. The pipeline picks up
> **any `.csv` inside `data/raw/`** (files starting with `US_Accidents` are preferred),
> so the exact name does not matter.

## Expected folder state
```
data/raw/US_Accidents_March23.csv      <- you add this
data/processed/accidents_clean.parquet <- created by the pipeline
data/sample/synthetic_accidents_sample.csv  <- provided, FAKE data for smoke tests only
```

## Columns the pipeline requires
`ID, Severity, Start_Time, Start_Lat, Start_Lng, City, State, Weather_Condition,
Sunrise_Sunset, Crossing, Junction, Railway, Traffic_Signal` (plus the other infrastructure
flags and weather measurements if present). If one is missing the program stops with a
clear message.

## About the sample file
`data/sample/synthetic_accidents_sample.csv` is **randomly generated** (`python -m src.make_sample`).
It imitates the column layout so you can test the code. It contains **no real accident
information**; never quote any number computed from it.

## Citation
Moosavi, S., Samavatian, M. H., Parthasarathy, S., & Ramnath, R. (2019). *A Countrywide
Traffic Accident Dataset.* arXiv:1906.05409.
Moosavi, S., et al. (2019). *Accident Risk Prediction based on Heterogeneous Sparse Data:
New Dataset and Insights.* ACM SIGSPATIAL. arXiv:1909.09638.
Check the dataset page for the licence/terms before use.
