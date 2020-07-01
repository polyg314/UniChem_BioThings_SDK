import pandas as pd
import os, csv, re
import numpy as np
from biothings.utils.dataload import dict_convert, dict_sweep
# from .yaml import yaml
from .csvsort import csvsort
# from .dask import dask
# import dask.dataframe as dd

from biothings import config
logging = config.logger

def load_annotations(data_folder):
    print("hellooooo")
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
    # read through file in chunks - too big to loadd all at once
    sdtype={'uci':'int32','standardinchikey':'str'}
    
    structure_df_chunk = pd.read_csv(struct_file, sep='\t', header=None, usecols=['uci', 'standardinchikey'],
                                         names=['uci_old','standardinchi','standardinchikey','created','username','fikhb','uci','parent_smiles'],
                                         chunksize=1000000, dtype=sdtype) 
    xref_chunk_list = []
    
    xdtype={'uci':'int32','src_id':'int8','src_compound_id':'str'}
    
    xref_df_chunk = pd.read_csv(xref_file, sep='\t', header=None, usecols=['uci','src_id','src_compound_id'],
                                         names=['uci_old','src_id','src_compound_id','assignment','last_release_u_when_current','created ','lastupdated','userstamp','aux_src','uci'],
                                         chunksize=1000000, dtype=xdtype) 
    
    # append structure chunks to list
    for chunk in structure_df_chunk:  
        structure_chunk_list.append(chunk)

    del structure_df_chunk

    # concat the list into dataframe 
    structure_df = pd.concat(structure_chunk_list)

    del structure_chunk_list

    # structure_df.to_csv(index=False, path_or_buf=os.path.join(data_folder,"structure_df.csv"))
    
    # del structure_df

    # same for xref chunks - list -> dataframe 
    merge_counter = 0;

    for chunk in xref_df_chunk:
        complete_df_chunk = pd.merge(left=structure_df, right=chunk, left_on='uci', right_on='uci')
        if(merge_counter == 0):
            complete_df_chunk.to_csv(path_or_buf=os.path.join(data_folder,"complete_df.csv"), index=False)
            merge_counter = 1;  
        else:
            complete_df_chunk.to_csv(path_or_buf=os.path.join(data_folder,"complete_df.csv"), index=False, mode='a', header=False)  
    #     xref_chunk_list.append(chunk)
    # del xref_df_chunk
    
    # xref_df = pd.concat(xref_chunk_list)

    # del xref_chunk_list

    del xref_df_chunk

    del complete_df_chunk

    # xref_df.to_csv(index=False, path_or_buf=os.path.join(data_folder,"xref_df.txt"))
    
    # del xref_df

    # sdf = dd.read_csv('structure_df.csv')
    # xdf = dd.read_csv('xref_df.csv')

    # df = dd.merge(sdf, xdf, on="uci").compute()

    # del sdf
    # del xdf

    # df.to_csv('complete_df.csv', index=False)

    # del df

    csvsort(os.path.join(data_folder,"complete_df.csv"),[0])

    # merge structure and xref dataframes by their UCI 
    # complete_df = pd.merge(left=complete_df, right=xref_df, left_on='uci', right_on='uci')

    # complete_df.to_csv(index=False, path_or_buf=os.path.join(data_folder,"complete_df_unsorted.txt"), sep="\t")


    # sort by their inchikey - make sure all rows with same inchi key are above/below each other
    # complete_df = complete_df.sort_values(by=['standardinchikey'])

    # complete_df.to_csv(index=False, path_or_buf=os.path.join(data_folder,"complete_df.txt"), sep="\t")
    
    # del complete_df

    new_entry = {}
    last_inchi = '';
    last_submitted_inchi = '1';

    # cd_type = {'uci':'int32','src_id':'int8','src_compound_id':'str'}
    complete_df_chunk = pd.read_csv(os.path.join(data_folder,"complete_df.csv"), chunksize=1000000)

    for chunk in complete_df_chunk:
        for row in chunk.itertuples(): 
            inchi = row[1]
            source = source_dict[row[3]]
            source_id = row[4]
            # check to see if previous entry had same inchi code. if so, 
            if(last_inchi == inchi):
                # if source id already exists for source, then create/add to list. if not, create first entry for source
                if(source in new_entry["unichem"]):
                    if(type(new_entry["unichem"][source]) == str):
                        new_entry["unichem"][source] = [new_entry["unichem"][source], source_id] 
                    else:
                        new_entry["unichem"][source].append(source_id) 
                else:
                    new_entry["unichem"][source] = source_id
            elif(len(last_inchi) == 0): 
                new_entry = {
                    "_id" : inchi,
                    "unichem": {
                        source: source_id
                    }
                }
                last_inchi = inchi
            else:
                yield new_entry;
                last_submitted_inchi = new_entry["_id"]
                new_entry = {
                    "_id" : inchi,
                    "unichem": {
                        source: source_id
                    }
                }
            last_inchi = inchi


    if(last_submitted_inchi != new_entry["_id"]):
        yield new_entry