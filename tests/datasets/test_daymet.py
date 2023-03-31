from springtime.datasets import daymet

ds = daymet.DaymetSinglePoint(point=(-87, 40), years=[2005, 2006])
ds.download()
raw_df = ds.load_raw()

import IPython; IPython.embed(); quit()

# TODO resample should add new column week number
# TODO resample should drop original date
# TODO resample should not attempt to average years (makes int float)

ds.load()
