from dtAcolite import dtAcolite

app_configuration = dtAcolite.configure_acolite_directory(base_dir = "../Test_dir", year = 2021, collection = "sentinel")
app_configuration
inputfilenames = dtAcolite.create_acolite_input(app_configuration = app_configuration)
outfilepaths   = dtAcolite.create_acolite_output(app_configuration=app_configuration, filenames=inputfilenames)
dtAcolite.unzip_inputfiles(app_configuration=app_configuration)