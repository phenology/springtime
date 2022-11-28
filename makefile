# Make sure all and clean are recognized as commands, not paths
.PHONY: all clean

# Make without target should make all:
all: data/raw/ppo_testdata.csv

# Clean up stuff
clean:
	# Clean python cache
	find . -type d -name "__pycache__" -exec rm -rf {} +

##### Data sets

# Test data from pyppo
data/raw/ppo_testdata.csv: scripts/download_ppo_data.py
	python scripts/download_ppo_data.py
