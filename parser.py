import pandas as pd
import os, csv, re
import numpy as np
from biothings.utils.dataload import dict_convert, dict_sweep

from biothings import config
logging = config.logger

def load_annotations(data_folder):
    # load source files
    source_file = os.path.join(data_folder,"UC_SOURCE.txt")
    struct_file = os.path.join(data_folder,"UC_STRUCTURE.txt")
    xref_file = os.path.join(data_folder,"UC_XREF.txt")
    assert os.path.exists(source_file)
    assert os.path.exists(struct_file)
    assert os.path.exists(xref_file)

    # create source dictionary, {source id: name of source}
    source_tsv = pd.read_csv(source_file, sep='\t', header= None)
    source_keys = list(source_tsv[0])
    source_values = list(source_tsv[1])
    source_dict = {source_keys[i]: source_values[i] for i in range(len(source_keys))}     

    # make lists of structure & xref chunks, to be concatenated later
    structure_chunk_list = []
    # read through file in chunks - too big to laod all at once
    structure_df_chunk = pd.read_csv(struct_file, sep='\t', header=None, usecols=['uci', 'standardinchikey'],
                                         names=['uci_old','standardinchi','standardinchikey','created','username','fikhb','uci','parent_smiles'],
                                         chunksize=1000000) 
    xref_chunk_list = []
    xref_df_chunk = pd.read_csv(xref_file, sep='\t', header=None, usecols=['uci','src_id','src_compound_id'],
                                         names=['uci_old','src_id','src_compound_id','assignment','last_release_u_when_current','created ','lastupdated','userstamp','aux_src','uci'],
                                         chunksize=1000000) 
    
    # append structure chunks to list
    for chunk in structure_df_chunk:  
        structure_chunk_list.append(chunk)

    # concat the list into dataframe 
    complete_df = pd.concat(structure_chunk_list)
    del structure_chunk_list
    # same for xref chunks - list -> dataframe 
    for chunk in xref_df_chunk:  
        xref_chunk_list.append(chunk)
    xref_df = pd.concat(xref_chunk_list)
    del xref_chunk_list
    # merge structure and xref dataframes by their UCI 
    complete_df = pd.merge(left=complete_df, right=xref_df, left_on='uci', right_on='uci')
    del xref_df
    # sort by their inchikey - make sure all rows with same inchi key are above/below each other
    complete_df = complete_df.sort_values(by=['standardinchikey'])
    
    # create final list containing each entry in json format
    final_array = [];
    last_inchi = ''
    for row in complete_df.itertuples(): 
        inchi = row[1]
        source = source_dict[row[3]]
        source_id = row[4]
        # check to see if previous entry had same inchi code. if so, 
        if(last_inchi == inchi):
            final_array[-1]["unichem"][source] = source_id
        else:
            new_entry = {
                "_id" : inchi,
                "unichem": {
                    source: source_id
                }
            }
            final_array.append(new_entry)

        last_inchi = inchi
    return final_array