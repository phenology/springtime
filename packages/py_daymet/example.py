from src import download, get_dataset, compute_stat
# Daymet is in 1 km resolution 
longitudes = [-80, -79.5]
latitudes = [35, 35.1]

var_names = ['dayl', 'prcp']
years = [2019, 2019]

# no download
data_arrays = get_dataset(longitudes, latitudes, var_names, years)
print(data_arrays)

# download
data_file_name = download(longitudes, latitudes, var_names, years)
print(data_file_name)

# calculate statistics here annual average 
data_frames = compute_stat(longitudes, latitudes, var_names, years)
print(data_frames)
