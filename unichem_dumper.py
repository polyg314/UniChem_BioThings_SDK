import sys
from .ftplib import ftplib

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

