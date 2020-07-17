import os

import biothings, config
biothings.config_for_app(config)
from config import DATA_ARCHIVE_ROOT

from biothings.utils.common import uncompressall

# import biothings.hub.dataload.dumper

# import sys
# from .ftplib import *

from biothings.hub.dataload.dumper import FTPDumper, DumperException

class Unichem_biothings_sdkDumper(FTPDumper):

    SRC_NAME = "UniChem_BioThings_SDK"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)    
    # SCHEDULE = None
    UNCOMPRESS = True
    # SRC_URLS = [
    #     'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI283/UC_SOURCE.txt.gz',
    #     'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI283/UC_STRUCTURE.txt.gz',
    #     'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI283/UC_XREF.txt.gz'
    # ]
    # self.SRC_URLS = [
    #     'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI283/UC_SOURCE.txt.gz',
    #     'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI283/UC_STRUCTURE.txt.gz',
    #     'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI283/UC_XREF.txt.gz'
    # ]

    def create_todump_list(self):
    	source_local = os.path.join(self.new_data_folder,"UC_SOURCE.txt.gz")
    	self.to_dump.append({"remote": 'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI283/UC_SOURCE.txt.gz',"local":local})

    __metadata__ = {"src_meta": {}}

    def post_dump(self, *args, **kwargs):
        if self.__class__.UNCOMPRESS:
            self.logger.info("Uncompress all archive files in '%s'" %
                             self.new_data_folder)
            uncompressall(self.new_data_folder)
