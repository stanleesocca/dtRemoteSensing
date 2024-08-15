from setuptools import setup

setup(name="dtRemote", 
      version="0.1.0", 
      author="Stanley Nmor", 
      author_email="stanley.nmor@nioz.nl",
      description="Remote sensing package for LTER-LIFE", 
      url="https://github.com/stanleesocca/dtRemoteSensing", 
      packages=["dtSat", "dtAcolite"], 
      install_requires= ["numpy",
                         "folium", 
                         "geojson", 
                         "geopandas", 
                         "pandas"], 
)