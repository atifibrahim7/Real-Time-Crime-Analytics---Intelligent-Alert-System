#!/bin/bash

# Create data directory if it doesn't exist
mkdir -p data

echo "Starting dataset downloads for Chicago Crime Analytics..."

# 1. Crime Data (Representative sample of 50,000 rows as per project guidelines)
# [cite: 180, 199]
echo "Downloading Crime Data (Sample)..."
curl -L "https://data.cityofchicago.org/resource/ijzp-q8t2.csv?\$limit=50000" -o data/crimes.csv

# 2. Police Stations (Full dataset is small)
# [cite: 200, 201]
echo "Downloading Police Stations..."
curl -L "https://data.cityofchicago.org/resource/z8bn-74gv.csv" -o data/police_stations.csv

# 3. Arrests (Sample of 10,000 rows)
# [cite: 180, 202, 203]
echo "Downloading Arrests..."
curl -L "https://data.cityofchicago.org/resource/dpt3-jri9.csv?\$limit=10000" -o data/arrests.csv

# 4. Violence Reduction (Sample of 10,000 rows)
# [cite: 180, 204, 205]
echo "Downloading Violence Data..."
curl -L "https://data.cityofchicago.org/resource/gumc-mgzr.csv?\$limit=10000" -o data/violence.csv

# 5. Sex Offenders (Sample of 10,000 rows)
# [cite: 180, 209, 210]
echo "Downloading Sex Offenders..."
curl -L "https://data.cityofchicago.org/resource/vc9r-bqvy.csv?\$limit=10000" -o data/sex_offenders.csv

echo "All downloads complete. Files located in ./data/"
ls -lh data/