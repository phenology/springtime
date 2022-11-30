from pyPhenology.models.utils.misc import temperature_only_data_prep
from pyPhenology.utils import load_test_data
import pandas as pd
from springtime import PROJECT_ROOT


def align_data(obs, pred):
    """Align the weather data with the observations."""
    # Apply PyPhenology's built-in preprocessing
    index, data, columns = temperature_only_data_prep(obs, pred)
    df = pd.DataFrame(data.T, index=index, columns=columns)

    # The returned data supposedly has the DOY column of obs as its index
    assert all(df.index == obs.doy)

    # To join dataframes, we want to make sure both indexes are aligned ()
    df = df.reset_index(drop=True)
    assert all(df.index == obs.index)

    # Now we can safely combine data
    transformed_df = pd.concat([obs, df], axis=1)

    return transformed_df


def main():
    """Load and pre-process the test data from PyPhenology.

    This will make it suitable for application of ML algorithms.
    """
    sample_datasets = [
        ["vaccinium", "budburst"],
        ["vaccinium", "flowers"],
        ["aspen", 'budburst'],
    ]

    for name, phenophase in sample_datasets:
        print(f"Processing dataset for {name}, {phenophase}")

        # Load and clean
        obs, pred = load_test_data(name=name, phenophase=phenophase)
        obs = obs.reset_index(drop=True).drop('phenophase', axis=1)

        # Prep
        combined_data = align_data(obs, pred)

        # Save
        combined_data.to_csv(f"{PROJECT_ROOT}/data/processed/pyphenology_{name}_{phenophase}.csv")


if __name__ == "__main__":
    main()
