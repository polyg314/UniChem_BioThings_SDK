import pandas as pd
import os, csv, re
import numpy as np
from biothings.utils.dataload import dict_convert, dict_sweep

from biothings import config
logging = config.logger

def load_annotations(data_folder):

    source_file = os.path.join(data_folder,"UC_SOURCE.txt")
    struct_file = os.path.join(data_folder,"UC_STRUCTURE_TEST.txt")
    xref_file = os.path.join(data_folder,"UC_XREF_TEST.txt")
    assert os.path.exists(source_file)
    assert os.path.exists(struct_file)
    assert os.path.exists(xref_file)

    # source_file = 'UC_SOURCE.txt'
    # struct_file = 'UC_STRUCTURE_TEST.txt'
    # xref_file = 'UC_XREF_TEST.txt'
    
    source_tsv = pd.read_csv(source_file, sep='\t', header= None)
    source_keys = list(source_tsv[0])
    source_values = list(source_tsv[1])
    source_dict = {source_keys[i]: source_values[i] for i in range(len(source_keys))}     
#     print(source_dict)

    structure_chunk_list = []
    structure_df_chunk = pd.read_csv(struct_file, sep='\t', header=None, usecols=['uci', 'standardinchikey'],
                                         names=['uci_old','standardinchi','standardinchikey','created','username','fikhb','uci','parent_smiles'],
                                         chunksize=1000000) 
    xref_chunk_list = []
    xref_df_chunk = pd.read_csv(xref_file, sep='\t', header=None, usecols=['uci','src_id','src_compound_id'],
                                         names=['uci_old','src_id','src_compound_id','assignment','last_release_u_when_current','created ','lastupdated','userstamp','aux_src','uci'],
                                         chunksize=1000000) 

    for chunk in structure_df_chunk:  
        structure_chunk_list.append(chunk)

    # concat the list into dataframe 
    structure_df = pd.concat(structure_chunk_list)

    for chunk in xref_df_chunk:  
        xref_chunk_list.append(chunk)

    # concat the list into dataframe 
    xref_df = pd.concat(xref_chunk_list)

    complete_df = pd.merge(left=structure_df, right=xref_df, left_on='uci', right_on='uci')

#     print(complete_df.shape)
#     print(complete_df.head())
#     print(complete_df.tail())

    complete_df_sorted = complete_df.sort_values(by=['standardinchikey'])

#     print(complete_df_sorted.shape)
#     print(complete_df_sorted.head())
#     print(complete_df_sorted.tail())

    counta = 0
    final_array = [];
    last_inchi = ''
    for row in complete_df_sorted.itertuples(): 
    #     counta = counta + 1;
    # #     print(counta)
        inchi = row[1]
        source = source_dict[row[3]]
        source_id = row[4]
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
        # if(counta%1000000 == 0):
        #     print("1M")

    # print(final_array[-1])
    for entry in final_array:
        yield entry
    