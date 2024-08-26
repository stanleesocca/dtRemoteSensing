import glob
import zipfile
import os
import re
import sys
import datetime
homepath = os.path.expanduser("~")
sys.path.append(homepath + "acolite")
import acolite as ac


def create_acolite_input(year, collection, inputdir = None, check_size = False):
    """
    This function create the acolite input files in the directory 'inputdir'. 
    
    =========================================================================
    Argument options
    =========================================================================
    year: year of analysis
    collection: satellite sensor, either 'sentinel'or 'landsat'. This argument should be same as the directory collection name
    inputdir: Directory where the input files are stored.
    check_size: experimental; Used for checking and filtering if the valid satellite files to downloaded 
    
    """
    collections = ["sentinel", "landsat"]
    if collection not in collections:
        print("collection but be either sentinel or landsat. Other satellite collection will be supported in the future.")
        return 
    
    raw_inputdir = f"./inputdir/{collection}/{year}"

    filenames = [file.split(".")[0] for file in os.listdir(raw_inputdir) if file.endswith(".zip")]
    
    if check_size:
        filenames = [file.split('.')[0] for file in os.listdir(raw_inputdir) if os.path.getsize(f"{raw_inputdir}/{file}")/1e6 >= 200.0]
    
    if not inputdir and collection: 
        acolite_inputdir = f"./app_acolite/inputdir/{collection}/{year}"
    else: 
        acolite_inputdir = inputdir
        
    if not os.path.exists(acolite_inputdir):
        os.makedirs(acolite_inputdir)
        print(f"acolite input directory {acolite_inputdir} is created...")
    else: 
        print(f"filepath {acolite_inputdir} is already created !!!")
    
    return filenames

def create_acolite_output(year, filenames, collection, outputdir = None):
    """
    This function create the acolite output files in the directory 'outputdir'. 
    
    =========================================================================
    Argument options
    =========================================================================
    year: year of analysis
    filenames: output from 'create_acolite_input' function consisting of the filenames. 
    collection: satellite sensor, either 'sentinel'or 'landsat'. This argument should be same as the directory collection name.
    outputdir: Directory where the output files are stored.
    
    """
    
    collections = ["sentinel", "landsat"]
    if collection not in collections:
        print("collection but be either sentinel or landsat. Other satellite collection will be supported in the future.")
        return 
    
    if not outputdir and collection: 
        acolite_outputdir = f"./app_acolite/outputdir/{collection}/{year}"
    else: 
        acolite_outputdir = outputdir
        
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

def unzip_inputfiles(inputdir = None, outputdir = None, check_size = False):
    
    """
    This function unzip all the downloaded files from inputdir and place them into outputdir 
    
    =========================================================================
    Argument options
    =========================================================================
    inputdir: Directory where the input files are stored.
    outputdir: Directory where the output files are stored. 
    check_size: experimental; Used for checking and filtering if valid satellite files is unzip. 
    """
    
    assert inputdir is not None, "input directory is not define" 
    
    if outputdir is None: 
        outputdir = inputdir 
        print(f"output directory is found in input directory {outputdir}")
    
    raw_scenes = [f"{inputdir}/{file}" for file in os.listdir(inputdir)]
    if check_size:
        raw_scenes = [f"{inputdir}/{file}" for file in os.listdir(inputdir) if os.path.getsize(f"{inputdir}/{file}")/1e6 >= 200.0]
        
    # filenames = [re.search("S2.*E", file.split(".z")[0]).group(0) for file in raw_scenes if file.endswith(".zip")]
    # filenames = [file for file in filenames if file not in os.listdir(outputdir)]
    
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