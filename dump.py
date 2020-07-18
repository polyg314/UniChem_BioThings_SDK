import os
import time

import biothings, config
biothings.config_for_app(config)

from config import DATA_ARCHIVE_ROOT

from biothings.utils.common import uncompressall

from biothings.hub.dataload.dumper import FTPDumper, DumperException

class Unichem_biothings_sdkDumper(FTPDumper):

    SRC_NAME = "UniChem_BioThings_SDK"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)  
    FTP_HOST = 'ftp.ebi.ac.uk'
    CWD_DIR = '/pub/databases/chembl/UniChem/data/oracleDumps'
    SCHEDULE = "0 6 * * *"
    UNCOMPRESS = True

    def get_newest_info(self):
        self.client.cwd("/pub/databases/chembl/UniChem/data/oracleDumps")
        releases = self.client.nlst()
        releases = [x.lstrip("UDRI") for x in releases if x.startswith('UDRI')]
        # sort items based on UDRI number - highest is most recent
        releases = sorted(releases)
        # get the last item in the list, which is the latest version
        self.release = releases[-1]

    def new_release_available(self):
    	try:
        	current_release = self.src_doc.get("download",{}).get("release")
        except:
        	# first download
        	current_release = False
        if not current_release or int(self.release) > int(current_release):
            self.logger.info("New release '%s' found" % self.release)
            return True
        else:
            self.logger.debug("No new release found")
            return False

    def create_todump_list(self, force=False):
        self.get_newest_info()
        for fn in ["UC_SOURCE.txt.gz","UC_STRUCTURE.txt.gz","UC_XREF.txt.gz"]:
            local_file = os.path.join(self.new_data_folder,fn)
            if force or not os.path.exists(local_file) or self.new_release_available():
                path = "/pub/databases/chembl/UniChem/data/oracleDumps/UDRI" + self.release + "/" + fn;
                self.to_dump.append({"remote": path, "local":local_file})

    def post_dump(self, *args, **kwargs):
        if self.__class__.UNCOMPRESS:
            self.logger.info("Uncompress all archive files in '%s'" % self.new_data_folder)
            uncompressall(self.new_data_folder)