from pydaymet import download, get_dataset
# Daymet is in 1 km resolution 
longitudes = [-80, -79.5]
latitudes = [35, 35.1]

var_names = ['dayl', 'prcp']
years = [2019]

# no download
data_arrays = get_dataset(longitudes, latitudes, var_names, years)
print(data_arrays)

# download
data_file_name = download(longitudes, latitudes, var_names, years)
print(data_file_name)

# calculate statistics here annual average 
def compute_stat(longitudes, latitudes, var_names, years):
 
    data_arrays = get_dataset(longitudes, latitudes, var_names, years)

    # TODO select statistics
    data_frames = []
    for data_array in data_arrays:
        # download starts, memory usage
        stat = data_array.groupby('time.year').mean('time')
        data_frames.append(stat.to_dataframe())
    return data_frames

data_frames = compute_stat(longitudes, latitudes, var_names, years)
print(data_frames)
