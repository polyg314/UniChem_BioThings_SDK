import os

import biothings, config
biothings.config_for_app(config)
from config import DATA_ARCHIVE_ROOT

from biothings.utils.common import uncompressall

import biothings.hub.dataload.dumper

import sys
from .ftplib import *





class Unichem_biothings_sdkDumper(biothings.hub.dataload.dumper.LastModifiedFTPDumper):


	SRC_NAME = "pubchem"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    FTP_HOST = 'ftp.ncbi.nlm.nih.gov'
    CWD_DIR = '/pubchem/Compound/CURRENT-Full/XML'
    ARCHIVE = False
    #SCHEDULE = "0 12 * * *"
    #MAX_PARALLEL_DUMP = 5

    VERSION_DIR = '/pubchem/Compound/Monthly'

    def get_release(self):
        try:
            self.client.cwd(self.__class__.VERSION_DIR)
            releases = sorted(self.client.nlst())
            if len(releases) == 0:
                raise DumperException("Can't any release information in '%s'" % self.__class__.VERSION_DIR)
            self.release = releases[-1]
        finally:
            self.client.cwd(self.__class__.CWD_DIR)

    def new_release_available(self):
        current_release = self.src_doc.get("download",{}).get("release")
        if not current_release or self.release > current_release:
            self.logger.info("New release '%s' found" % self.release)
            return True
        else:
            self.logger.debug("No new release found")
            return False

    def create_todump_list(self, force=False):
        self.get_release()
        if force or self.new_release_available():
            # get list of files to download
            remote_files = self.client.nlst()
            for remote in remote_files:
                try:
                    local = os.path.join(self.new_data_folder,remote)
                    if not os.path.exists(local) or self.remote_is_better(remote,local):
                        self.to_dump.append({"remote": remote,"local":local})
                except ftplib.error_temp as e:
                    self.logger.debug("Recycling FTP client because: '%s'" % e)
                    self.release_client()
                    self.prepare_client()

 #    SRC_NAME = "UniChem_BioThings_SDK"
 #    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
 #    SCHEDULE = None
 #    UNCOMPRESS = True
 #    FTP_HOST = 'ftp.ebi.ac.uk'
 #    CWD_DIR = '/pub/databases/chembl/UniChem/data/oracleDumps/UDRI283'
 #    # SRC_URLS = [
 #    #     'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI283/UC_SOURCE.txt.gz',
 #    #     'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI283/UC_STRUCTURE.txt.gz',
 #    #     'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI283/UC_XREF.txt.gz'
 #    # ]
 #    # __metadata__ = {"src_meta": {}}

 #    def get_latest_unichem_urls():
	# 	"""Function largely borrowed from 
	# 	https://github.com/TranslatorIIPrototypes/Babel/blob/master/babel/unichem/unichem.py
	# 	"""

	# 	# get a handle to the ftp directory
	# 	ftp = ftplib.FTP("ftp.ebi.ac.uk")

	# 	# login
	# 	ftp.login()

	# 	# move to the target directory
	# 	ftp.cwd('/pub/databases/chembl/UniChem/data/oracleDumps')

	# 	# get the directory listing
	# 	files: list = ftp.nlst()

	# 	# close the ftp connection
	# 	ftp.quit()

	# 	# init the starting point
	# 	target_dir_index = 0

	# 	# parse the list to determine the latest version of the files
	# 	for f in files:
	# 		# is this file greater that the previous
	# 		if "UDRI" in f:
	# 			# convert the suffix into an int and compare it to the previous one
	# 			if int(f[4:]) > target_dir_index:
	# 				# save this as our new highest value
	# 				target_dir_index = int(f[4:])

	# 	# make urls for source, structure, and xref files 
	# 	source = 'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI' + target_dir_index + '/UC_SOURCE.txt.gz';
	# 	structure = 'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI' + target_dir_index + '/UC_STRUCTURE.txt.gz';
	# 	xref = 'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI' + target_dir_index + '/UC_XREF.txt.gz';

	# 	return([source, structure, xref])

	# def create_todump_list(self, force=False):
	#         # self.get_newest_info()
	#         for fn in ["UC_SOURCE.txt.gz",
	#                 "UC_STRUCTURE.txt.gz",
	#                 "UC_XREF.txt.gz"]:
	#             # local_file = os.path.join(self.new_data_folder,fn)
	#             # if force or not os.path.exists(local_file) or self.remote_is_better(fn,local_file) or self.new_release_available():
	#             self.to_dump.append({"remote": fn, "local":local_file})

 #    def post_dump(self, *args, **kwargs):
 #        if self.__class__.UNCOMPRESS:
 #            self.logger.info("Uncompress all archive files in '%s'" %
 #                             self.new_data_folder)
 #            uncompressall(self.new_data_folder)
