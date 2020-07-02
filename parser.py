import pandas as pd
import os, csv, re
import numpy as np
from biothings.utils.dataload import dict_convert, dict_sweep
# from .yaml import yaml
# from .yaml.error import error
from .csvsort import csvsort
# from .dask import dask

# import dask.dataframe as dd

from biothings import config
logging = config.logger

def load_annotations(data_folder):

    current_chunk_size = 100000;
    # load source files
    source_file = os.path.join(data_folder,"UC_SOURCE.txt")
    struct_file = os.path.join(data_folder,"UC_SP900.txt")
    xref_file = os.path.join(data_folder,"UC_XP900.txt")
    assert os.path.exists(source_file)
    assert os.path.exists(struct_file)
    assert os.path.exists(xref_file)

    # create source dictionary, {source id: name of source}
    source_tsv = pd.read_csv(source_file, sep='\t', header= None)
    source_keys = list(source_tsv[0])
    source_values = list(source_tsv[1])
    source_dict = {source_keys[i]: source_values[i] for i in range(len(source_keys))}     

    
    ## make structure file 

    # read through file in chunks - too big to loadd all at once
    sdtype={'uci':'int64','standardinchikey':'str'}
    
    structure_df_chunk = pd.read_csv(struct_file, sep='\t', header=None, usecols=['uci', 'standardinchikey'],
                                         names=['uci_old','standardinchi','standardinchikey','created','username','fikhb','uci','parent_smiles'],
                                         chunksize=current_chunk_size, dtype=sdtype) 

    
    # append structure chunks to list
    smerge_counter = 0;

    for chunk in structure_df_chunk:
        if(smerge_counter == 0):
            chunk.to_csv(path_or_buf=os.path.join(data_folder,"structure_df.csv"), index=False)
            smerge_counter = 1;  
        else:
            chunk.to_csv(path_or_buf=os.path.join(data_folder,"structure_df.csv"), index=False, mode='a', header=False)    

    del structure_df_chunk

    csvsort(os.path.join(data_folder,"structure_df.csv"),[1],numeric_column=True)

    ## make xref file   
    
    xdtype={'src_id':'int8','src_compound_id':'str','uci':'int64'}
    
    xref_df_chunk = pd.read_csv(xref_file, sep='\t', header=None, usecols=['src_id','src_compound_id', 'uci'],
                                         names=['uci_old','src_id','src_compound_id','assignment','last_release_u_when_current','created ','lastupdated','userstamp','aux_src','uci'],
                                         chunksize=current_chunk_size, dtype=xdtype)  

    xmerge_counter = 0;

    for chunk in xref_df_chunk:
        if(xmerge_counter == 0):
            chunk.to_csv(path_or_buf=os.path.join(data_folder,"xref_df.csv"), index=False)
            xmerge_counter = 1;  
        else:
            chunk.to_csv(path_or_buf=os.path.join(data_folder,"xref_df.csv"), index=False, mode='a', header=False)    


    csvsort(os.path.join(data_folder,"xref_df.csv"),[2],numeric_column=True)

    chunk_counter = 0;
    
    structure_df_chunk = pd.read_csv(os.path.join(data_folder,"structure_df.csv"), chunksize=current_chunk_size)
    min_max_columns = ["chunk_start", "min_uci", "max_uci"];
    structure_min_max_df = pd.DataFrame(columns = min_max_columns)
    for schunk in structure_df_chunk:
        chunk_start = chunk_counter*current_chunk_size
        chunk_min = min(schunk["uci"])
        chunk_max = max(schunk["uci"])
        chunk_counter = chunk_counter + 1;
        structure_min_max_df = structure_min_max_df.append(pd.DataFrame([[chunk_start,chunk_min,chunk_max]], columns = min_max_columns))
    
    xdf_chunk = pd.read_csv(os.path.join(data_folder,"xref_df.csv"), chunksize=current_chunk_size) 

    merge_counter = 0; 
    for xchunk in xdf_chunk:
        current_x_min = min(xchunk["uci"])
        current_x_max = max(xchunk["uci"])
        for index, row in structure_min_max_df.iterrows():
            if(not((current_x_max < row['min_uci']) or (current_x_min > row['max_uci']))):
                sdf_chunk = pd.read_csv(os.path.join(data_folder,"structure_df.csv"), skiprows = row["chunk_start"], header=0, names=['standardinchikey','uci'], nrows=current_chunk_size)
                complete_df_chunk = pd.merge(left=sdf_chunk, right=xchunk, left_on='uci', right_on='uci')
                if(merge_counter == 0):
                    complete_df_chunk.to_csv(path_or_buf=os.path.join(data_folder,"complete_df.csv"), index=False)
                    merge_counter = 1;  
                else:
                    complete_df_chunk.to_csv(path_or_buf=os.path.join(data_folder,"complete_df.csv"), index=False, mode='a', header=False)
        
        
    del sdf_chunk
    del xdf_chunk 

    
    csvsort(os.path.join(data_folder,"complete_df.csv"),[0], numeric_column = False)

    new_entry = {}
    last_inchi = '';
    last_submitted_inchi = '1';

    complete_df_chunk = pd.read_csv(os.path.join(data_folder,"complete_df.csv"), chunksize=current_chunk_size)

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