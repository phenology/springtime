"""Download data from ppo and plot it on a map.

Use as:

  > python download_ppo_data.py

"""
import matplotlib.pyplot as plt
from cartopy import crs
from py_ppo import download
from pathlib import Path
from springtime import PROJECT_ROOT, Dataset


def flowers_present(df):
    return df.termID.str.contains("obo:PPO_0002330")


def true_leaves_present(df):
    return df.termID.str.contains("obo:PPO_0002313")


def senescing_true_leaves_present(df):
    return df.termID.str.contains("obo:PPO_0002317")


def leafing_out_date(df):
    condition = true_leaves_present and ~senescing_true_leaves_present(df)
    unique_idx = ['latitude', 'longitude', 'year']
    event = df.where(condition).groupby(unique_idx).dayOfYear.min()
    return event.reset_index()


if __name__ == "__main__":

    df = download(
        genus="Syringa",
        source="PEP725",
        year="[2000 TO 2021]",
        latitude="[40 TO 70]",
        longitude="[-10 TO 40]",
        termID="obo:PPO_0002313",
        explode=False,
        limit=1535,
        timeout=10,
        )

    relative_path = f"data/raw/ppo_testdata.csv"
    df.to_csv(f"{PROJECT_ROOT}/{relative_path}")

    # Register metadata in local database
    dataset = Dataset(
        name = f"ppo_example_data",
        description = "Example dataset downloaded using py_ppo.",
        path = relative_path,
        license = "See http://biscicol.org/",
        provenance = "springtime/scripts/download_ppo_data.py",
        reference = "Original source: http://biscicol.org/",
    )
    dataset.add_to_database()


    event = leafing_out_date(df)
    fig = plt.figure()
    ax = fig.add_subplot(projection=crs.PlateCarree())
    ax.coastlines()
    ax.set_xlim(-10, 40)
    ax.set_ylim(40, 70)
    ax.scatter(event.longitude, event.latitude, c=event.dayOfYear)
    fig.savefig('test_image', bbox_inches='tight')
