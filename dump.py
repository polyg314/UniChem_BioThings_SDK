import os

import biothings, config
biothings.config_for_app(config)
from config import DATA_ARCHIVE_ROOT

from biothings.utils.common import uncompressall

import biothings.hub.dataload.dumper

import sys
from .ftplib import *





class Unichem_biothings_sdkDumper(biothings.hub.dataload.dumper.LastModifiedFTPDumper):

    SRC_NAME = "UniChem_BioThings_SDK"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    SCHEDULE = None
    UNCOMPRESS = True
    SRC_URLS = self.get_latest_unichem_urls()

    #SCHEDULE = "0 12 * * *"

    __metadata__ = {"src_meta": {}}

    def get_latest_unichem_urls():
		"""Function largely borrowed from 
		https://github.com/TranslatorIIPrototypes/Babel/blob/master/babel/unichem/unichem.py
		"""

		# get a handle to the ftp directory
		ftp = ftplib.FTP("ftp.ebi.ac.uk")

		# login
		ftp.login()

		# move to the target directory
		ftp.cwd('/pub/databases/chembl/UniChem/data/oracleDumps')

		# get the directory listing
		files: list = ftp.nlst()

		# close the ftp connection
		ftp.quit()

		# init the starting point
		target_dir_index = 0

		# parse the list to determine the latest version of the files
		for f in files:
			# is this file greater that the previous
			if "UDRI" in f:
				# convert the suffix into an int and compare it to the previous one
				if int(f[4:]) > target_dir_index:
					# save this as our new highest value
					target_dir_index = int(f[4:])

		# make urls for source, structure, and xref files 
		source = 'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/' + f + '/UC_SOURCE.txt.gz';
		structure = 'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/' + f + '/UC_STRUCTURE.txt.gz';
		xref = 'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/' + f + '/UC_XREF.txt.gz';

		return([source, structure, xref])

    def post_dump(self, *args, **kwargs):
        if self.__class__.UNCOMPRESS:
            self.logger.info("Uncompress all archive files in '%s'" %
                             self.new_data_folder)
            uncompressall(self.new_data_folder)
