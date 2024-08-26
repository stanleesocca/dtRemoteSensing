import glob
import zipfile
import os
import re
import sys
import datetime

homepath = os.path.expanduser("~")
sys.path.append(homepath + "acolite")
import acolite as ac


def configure_acolite_directory(base_dir = None, collection = None, year = None):
    """
    create global configuration of acolite input and output directory structure from 
    base directory and collection type and year. 

    =========================================================================
    Argument options
    =========================================================================
    base_dir: Base directory upon which the application/configuration for dtSat and dtAcolite will work 
    collection: satellite sensor, either 'sentinel'or 'landsat'. This argument should be same as the directory collection name
    year: year of analysis

    =========================================================================
    OUTPUT: 
    =========================================================================
    app_configuration: Dictionary of raw input directory, processed input and output directory. Provide mapping for 
                        further downstream application.

    =========================================================================
    Example: 
    =========================================================================
    from dtAcolite import dtAcolite

    app_configuration = dtAcolite.configure_acolite_directory(base_dir = "./test_dir", year = 2021, collection = "sentinel")
    
    """

    if not base_dir: return "Please provide the base directory where you want the processing to be done. "
    if not collection: return "Please provide the satellite collection: either 'sentinel' or 'landsat'"
    if not year: return "Please provide the year of the collection. "

    raw_inputdir = base_dir + f"/app_acolite/raw/{collection}/{year}"
    acolite_inputdir = base_dir + f"/app_acolite/processed/inputdir/{collection}/{year}"
    acolite_outputdir = base_dir + f"/app_acolite/processed/outputdir/{collection}/{year}"


    if not os.path.exists(raw_inputdir):
        os.makedirs(raw_inputdir)
        print(f"Raw data for {collection} directory {raw_inputdir} is created...")
    else: 
        print(f"filepath {raw_inputdir} is already created !!!")

    if not os.path.exists(acolite_inputdir):
        os.makedirs(acolite_inputdir)
        print(f"acolite input directory {acolite_inputdir} is created...")
    else: 
        print(f"filepath {acolite_inputdir} is already created !!!")


    if not os.path.exists(acolite_outputdir):
        os.makedirs(acolite_outputdir)
        print(f"acolite input directory {acolite_outputdir} is created...")
    else: 
        print(f"filepath {acolite_outputdir} is already created !!!")


    app_configuration = {
        "year" : year, 
        "collection" : collection, 
        "raw_inputdir" : raw_inputdir, 
        "acolite_inputdir" : acolite_inputdir, 
        "acolite_outputdir" : acolite_outputdir
    }

    return app_configuration


def create_acolite_input(app_configuration = {}):
    """
    This function create the acolite input files in the directory 'inputdir'. 
    
    =========================================================================
    Argument options
    =========================================================================
    app_configuration: Dictionary of configuration object containing the mapping relationship of input and output directory 

    =========================================================================
    OUTPUT: 
    =========================================================================
    filenames: List of filename(s) of the raw satellite zipped data. This filename(s) is used for creating individual
               output director(ies) for the processed images. 
    =========================================================================
    Example: 
    =========================================================================
    from dtAcolite import dtAcolite

    app_configuration = dtAcolite.configure_acolite_directory(base_dir = "./test_dir", year = 2021, collection = "sentinel")
    inputfilenames = dtAcolite.create_acolite_input(app_configuration = app_configuration)
    
    """

    collection = app_configuration["collection"]
    raw_inputdir = app_configuration["raw_inputdir"]
    acolite_inputdir = app_configuration["acolite_inputdir"]

    collections = ["sentinel", "landsat"]
    if collection not in collections:
        print("collection but be either sentinel or landsat. Other satellite collection will be supported in the future.")
        return 
    
    if not raw_inputdir or raw_inputdir is None: 
        return "Please provide the location of the input files."
    
    if not acolite_inputdir or acolite_inputdir is None: 
        return "Please provide the location of the input files."

    filenames = [file.split(".")[0] for file in os.listdir(raw_inputdir) if file.endswith(".zip")]
        
        
    if not os.path.exists(acolite_inputdir):
        os.makedirs(acolite_inputdir)
        print(f"acolite input directory {acolite_inputdir} is created...")
    else: 
        print(f"filepath {acolite_inputdir} is already created !!!")
    
    return filenames

def create_acolite_output(filenames, app_configuration):
    """
    This function create the acolite output files in the directory 'outputdir'. 
    
    =========================================================================
    Argument options
    =========================================================================
    filenames: List of filename(s) need to create individual directory for processed image. 
    app_configuration: Dictionary of configuration object containing the mapping relationship of input and output directory 

    =========================================================================
    OUTPUT: 
    =========================================================================
    outfilepaths: List of output filepath(s) encoding the output directory of the processed image. This filepath(s) is 
                used in batch-processing atmospheric correction by acolite.
    
    =========================================================================
    Example: 
    =========================================================================
    from dtAcolite import dtAcolite

    app_configuration = dtAcolite.configure_acolite_directory(base_dir = "./test_dir", year = 2021, collection = "sentinel")
    inputfilenames = dtAcolite.create_acolite_input(app_configuration = app_configuration)
    outfilepaths   = dtAcolite.create_acolite_output(app_configuration=app_configuration, filenames=inputfilenames)
    
    """

    collection = app_configuration["collection"]
    acolite_outputdir = app_configuration["acolite_outputdir"]
    
    collections = ["sentinel", "landsat"]
    if collection not in collections:
        print("collection but be either sentinel or landsat. Other satellite collection will be supported in the future.")
        return 
        
    if not os.path.exists(acolite_outputdir):
        print(f"output directory does not exist, creating a new directory {acolite_outputdir}")
        os.makedirs(acolite_outputdir)
        
        
    outfilepaths = []
    for file in filenames:
        filepath = f"{acolite_outputdir}/{file}"
        outfilepaths.append(filepath)
        if not os.path.exists(filepath):
            os.makedirs(filepath)
            print(f"filepath {filepath} created !!!")
        else: 
            print(f"filepath {filepath} is already created !!!")
            
    return outfilepaths

def unzip_inputfiles(app_configuration = {}):
    
    """
    This function unzip all the downloaded files from inputdir and place them into outputdir 
    
    =========================================================================
    Argument options
    ========================================================================= 
    app_configuration: Dictionary of configuration object containing the mapping relationship of input and output directory 

    =========================================================================
    OUTPUT: 
    =========================================================================
    None. Just unzip the file(s) to the relevant location as provided by the app_configuration
    
    =========================================================================
    Example: 
    =========================================================================
    from dtAcolite import dtAcolite

    app_configuration = dtAcolite.configure_acolite_directory(base_dir = "./test_dir", year = 2021, collection = "sentinel")
    inputfilenames = dtAcolite.create_acolite_input(app_configuration = app_configuration)
    outfilepaths   = dtAcolite.create_acolite_output(app_configuration=app_configuration, filenames=inputfilenames)
    dtAcolite.unzip_inputfiles(app_configuration=app_configuration)
    """

    inputdir = app_configuration["raw_inputdir"]
    outputdir = app_configuration["acolite_inputdir"]
    
    assert inputdir is not None, "input directory is not define" 
    
    if outputdir is None: 
        outputdir = inputdir 
        print(f"output directory is found in input directory {outputdir}")
    
    raw_scenes = [f"{inputdir}/{file}" for file in os.listdir(inputdir)]
    
    raw_scenes = [file for file in raw_scenes if file.endswith(".zip") and re.search("S2.*E", file.split(".z")[0]).group(0) not in os.listdir(outputdir)]

    for i in range(len(raw_scenes)):
        with zipfile.ZipFile(raw_scenes[i], 'r') as zip_ref:
            zip_ref.extractall(outputdir)
            print(f"{i+1}: Downloaded file unzipping completed!!!!") 
    
    
def acolite_batch_run(settings, inputfile, outputdir):
    
    """
    This function run acolite program in batch mode. 
    
    =========================================================================
    Argument options
    =========================================================================
    settings: A dictionary of user settings (see acolite manual)
    inputfile: Directory where the input files are stored. 
    outputdir: Directory where the output files are stored.
    """

    for i in range(len(inputfile)):
        print("---------------------------------------------------------------------------------------")
        settings['inputfile'] = inputfile[i]
        settings['output'] = outputdir[i]
        ac.acolite.acolite_run(settings=settings)
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print(f"processing done and output is in {inputfile[i]}")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    
    
def find_remaining_files(year, inputdir = None, outputdir = None):
    
    """
    This utility function find the remaining files which were not downloaded due to network problem during downloading 
    
    =========================================================================
    Argument options
    =========================================================================
    year: Year of analysis 
    inputdir: Directory where the input files are stored. 
    outputdir: Directory where the output files are stored.
    
    """

    # assert year is not None | year != "", "year is empty"
    
    if not inputdir: 
        acolite_inputdir = f"./app_acolite/inputdir/landsat/{year}"
    else: 
        acolite_inputdir = inputdir 
        
    if not outputdir: 
        acolite_outputdir = f"./app_acolite/outputdir/landsat/{year}"
    else: 
        acolite_outputdir = outputdir
    
    dirlist = glob.glob(f"{acolite_outputdir}/**")
    dirlist
    idx = []
    for i in range(len(dirlist)):
        all_files = os.listdir(dirlist[i])
        is_L2W = any([re.search(r'.*_(L2W)+', file) for file in all_files])
        if not is_L2W:
            idx.append(i)
    return idx


def create_datetime_from_isodate(isodate):
    ymd_data = isodate.split("T")[0]
    hms_data = re.split("\.|\+", re.split("T", isodate)[1])[0]
    return datetime.datetime.strptime(f"{ymd_data} {hms_data}", "%Y-%m-%d %H:%M:%S")


def map_acolite_chla(year, collection, logunit = False):
    
    """
    Utility function mapping Chlorophyll-a estimated by acolite.
    
    =========================================================================
    Argument options
    =========================================================================
    year: Year of analysis
    collection: satellite sensor, either 'sentinel'or 'landsat'. This argument should be same as the directory collection name.
    logunit: Whether chlorophyll-a should be log-transformed. 
    """
    
    collections = ["sentinel", "landsat"]
    if collection not in collections:
        print("collection but be either sentinel or landsat. Other satellite collection will be supported in the future.")
        return 
    
    from math import ceil, sqrt
    ncfs = glob.glob(f"./app_acolite/outputdir/{collection}/{year}/**/L8**L2W**")
    nv = len(ncfs)
    print(nv)
    nc = min(ceil(sqrt(nv)), 4)
    nr = max(ceil(nv/nc), 4)
    fig, axes = plt.subplots(ncols = nc, nrows = nr, figsize=(15, 5))
    axes = axes.flatten()

    for i, ax in enumerate(axes):
        if i >= nv: break 
        nc = Dataset(ncfs[i])
        nc.variables.keys()
        lon = nc['lon'][:]
        lat = nc['lat'][:]
        chla = nc['chl_oc3'][:]
        isodate = nc.getncattr("isodate")
        nc.close()
        axes[i].set_title(f"Chla at {isodate}")
        if logunit:
            pcm = axes[i].contourf(lon, lat, np.log(chla))
        else:
            pcm = axes[i].contourf(lon, lat, chla)
        fig.colorbar(pcm, ax = axes[i])
    plt.show()