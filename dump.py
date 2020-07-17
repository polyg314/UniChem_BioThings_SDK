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
 #    FTP_HOST = 'ftp.ebi.ac.uk'
	# CWD_DIR = '/pub/databases/chembl/UniChem/data/oracleDumps/UDRI283'
 #    VERSION_DIR = '/pub/databases/chembl/UniChem/data/oracleDumps'


    # def get_release(self):
    #     try:
    #         self.client.cwd(self.__class__.VERSION_DIR)
    #         releases = sorted(self.client.nlst())
    #         if len(releases) == 0:
    #             raise DumperException("Can't any release information in '%s'" % self.__class__.VERSION_DIR)
    #         self.release = releases[-1]
    #     finally:
    #         self.client.cwd(self.__class__.CWD_DIR)

    # def new_release_available(self):
    #     current_release = self.src_doc.get("download",{}).get("release")
    #     if not current_release or self.release > current_release:
    #         self.logger.info("New release '%s' found" % self.release)
    #         return True
    #     else:
    #         self.logger.debug("No new release found")
    #         return False

    # def create_todump_list(self, force=False):
    #     self.get_release()
    #     if force or self.new_release_available():
    #         # get list of files to download
    #         remote_files = self.client.nlst()
    #         for remote in remote_files:
    #             try:
    #                 local = os.path.join(self.new_data_folder,remote)
    #                 if not os.path.exists(local) or self.remote_is_better(remote,local):
    #                     self.to_dump.append({"remote": remote,"local":local})
    #             except ftplib.error_temp as e:
    #                 self.logger.debug("Recycling FTP client because: '%s'" % e)
    #                 self.release_client()
    #                 self.prepare_client()

                    

    def create_todump_list(self, force=False):
    	# self.get_release()
    	source_local = os.path.join(self.new_data_folder,"UC_SOURCE.txt.gz")
    	self.to_dump.append({"remote": 'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI283/UC_SOURCE.txt.gz',"local":local})

    # __metadata__ = {"src_meta": {}}

    def post_dump(self, *args, **kwargs):
        if self.__class__.UNCOMPRESS:
            self.logger.info("Uncompress all archive files in '%s'" %
                             self.new_data_folder)
            uncompressall(self.new_data_folder)
