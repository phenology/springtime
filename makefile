# Make sure all and clean are recognized as commands, not paths
.PHONY: all clean

# Make without target should make all:
all: database data

# Clean up stuff
clean:
	# Clean python cache
	find . -type d -name "__pycache__" -exec rm -rf {} +

database: packages/springtime/src/springtime/database.py
	python packages/springtime/src/springtime/database.py

##### Data sets
data: \
data/raw/ppo_testdata.csv \
data/processed/pyphenology_vaccinium_budburst.csv \
data/processed/pyphenology_vaccinium_flowers.csv \
data/processed/pyphenology_aspen_budburst.csv

# Test data from pyppo
data/raw/ppo_testdata.csv: scripts/download_ppo_data.py
	python scripts/download_ppo_data.py

# Test data from pyphenology
data/processed/pyphenology_vaccinium_budburst.csv: scripts/dataprep_pyphenology_testdata.py
	python scripts/dataprep_pyphenology_testdata.py

data/processed/pyphenology_vaccinium_flowers.csv: scripts/dataprep_pyphenology_testdata.py
	python scripts/dataprep_pyphenology_testdata.py

data/processed/pyphenology_aspen_budburst.csv: scripts/dataprep_pyphenology_testdata.py
	python scripts/dataprep_pyphenology_testdata.py
