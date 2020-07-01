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





# # csvsort functions

# import csv, heapq, logging, multiprocessing, os, sys, tempfile
# if sys.version_info.major == 2:
#     from io import open
# from optparse import OptionParser
# csv.field_size_limit(2**30) # can't use sys.maxsize because of Windows error


# class CsvSortError(Exception):
#     pass


# def csvsort(input_filename,
#             columns,
#             output_filename=None,
#             max_size=100,
#             has_header=True,
#             delimiter=',',
#             show_progress=False,
#             parallel=True,
#             quoting=csv.QUOTE_MINIMAL,
#             encoding=None):
#     """Sort the CSV file on disk rather than in memory.

#     The merge sort algorithm is used to break the file into smaller sub files

#     Args:
#         input_filename: the CSV filename to sort.
#         columns: a list of columns to sort on (can be 0 based indices or header
#             keys).
#         output_filename: optional filename for sorted file. If not given then
#             input file will be overriden.
#         max_size: the maximum size (in MB) of CSV file to load in memory at
#             once.
#         has_header: whether the CSV contains a header to keep separated from
#             sorting.
#         delimiter: character used to separate fields, default ','.
#         show_progress (Boolean): A flag whether or not to show progress.
#             The default is False, which does not print any merge information.
#         quoting: How much quoting is needed in the final CSV file.  Default is
#             csv.QUOTE_MINIMAL.
#         encoding: The name of the encoding to use when opening or writing the
#             csv files. Default is None which uses the system default.
#     """

#     with open(input_filename, newline='', encoding=encoding) as input_fp:
#         reader = csv.reader(input_fp, delimiter=delimiter)
#         if has_header:
#             header = next(reader)
#         else:
#             header = None

#         columns = parse_columns(columns, header)

#         filenames = csvsplit(reader, max_size)
#         if show_progress:
#             logging.info('Merging %d splits' % len(filenames))

#         if parallel:
#             concurrency = multiprocessing.cpu_count()
#             with multiprocessing.Pool(processes=concurrency) as pool:
#                 map_args = [(filename, columns, encoding) for filename in filenames]
#                 pool.starmap(memorysort, map_args)
#         else:
#             for filename in filenames:
#                 memorysort(filename, columns, encoding)
#         sorted_filename = mergesort(filenames, columns, encoding=encoding)

#     # XXX make more efficient by passing quoting, delimiter, and moving result
#     # generate the final output file
#     with open(output_filename or input_filename, 'w', newline='', encoding=encoding) as output_fp:
#         writer = csv.writer(output_fp, delimiter=delimiter, quoting=quoting)
#         if header:
#             writer.writerow(header)
#         with open(sorted_filename, newline='', encoding=encoding) as sorted_fp:
#             for row in csv.reader(sorted_fp):
#                 writer.writerow(row)

#     os.remove(sorted_filename)


# def parse_columns(columns, header):
#     """check the provided column headers
#     """
#     for i, column in enumerate(columns):
#         if isinstance(column, int):
#             if header:
#                 if column >= len(header):
#                     raise CsvSortError(
#                         'Column index is out of range: "{}"'.format(column))
#         else:
#             # find index of column from header
#             if header is None:
#                 raise CsvSortError(
#                     'CSV needs a header to find index of this column name:' +
#                     ' "{}"'.format(column))
#             else:
#                 if column in header:
#                     columns[i] = header.index(column)
#                 else:
#                     raise CsvSortError(
#                         'Column name is not in header: "{}"'.format(column))
#     return columns


# def csvsplit(reader, max_size):
#     """Split into smaller CSV files of maximum size and return the filenames.
#     """
#     max_size = max_size * 1024 * 1024  # convert to bytes
#     writer = None
#     current_size = 0
#     split_filenames = []

#     # break CSV file into smaller merge files
#     for row in reader:
#         if writer is None:
#             ntf = tempfile.NamedTemporaryFile(delete=False, mode='w')
#             writer = csv.writer(ntf)
#             split_filenames.append(ntf.name)

#         writer.writerow(row)
#         current_size += sys.getsizeof(row)
#         if current_size > max_size:
#             writer = None
#             current_size = 0
#     return split_filenames


# def memorysort(filename, columns, encoding=None):
#     """Sort this CSV file in memory on the given columns
#     """
#     with open(filename, newline='', encoding=encoding) as input_fp:
#         rows = [row for row in csv.reader(input_fp) if row]
#     rows.sort(key=lambda row: get_key(row, columns))
#     with open(filename, 'w', newline='', encoding=encoding) as output_fp:
#         writer = csv.writer(output_fp)
#         for row in rows:
#             writer.writerow(row)


# def get_key(row, columns):
#     """Get sort key for this row
#     """
#     return [row[column] for column in columns]


# def decorated_csv(filename, columns, encoding=None):
#     """Iterator to sort CSV rows
#     """
#     with open(filename, newline='', encoding=encoding) as fp:
#         for row in csv.reader(fp):
#             yield get_key(row, columns), row


# def mergesort(sorted_filenames, columns, nway=2, encoding=None):
#     """Merge these 2 sorted csv files into a single output file
#     """
#     merge_n = 0
#     while len(sorted_filenames) > 1:
#         merge_filenames, sorted_filenames = \
#            sorted_filenames[:nway], sorted_filenames[nway:]

#         with tempfile.NamedTemporaryFile(delete=False, mode='w') as output_fp:
#             writer = csv.writer(output_fp)
#             merge_n += 1
#             for _, row in heapq.merge(*[decorated_csv(filename, columns, encoding)
#                                         for filename in merge_filenames]):
#                 writer.writerow(row)

#             sorted_filenames.append(output_fp.name)

#         for filename in merge_filenames:
#             os.remove(filename)
#     return sorted_filenames[0]


# def main():
#     parser = OptionParser()
#     parser.add_option(
#         '-c',
#         '--column',
#         dest='columns',
#         action='append',
#         help='column of CSV to sort on')
#     parser.add_option(
#         '-s',
#         '--size',
#         dest='max_size',
#         type='float',
#         default=100,
#         help='maximum size of each split CSV file in MB (default 100)')
#     parser.add_option(
#         '-n',
#         '--no-header',
#         dest='has_header',
#         action='store_false',
#         default=True,
#         help='set CSV file has no header')
#     parser.add_option(
#         '-d',
#         '--delimiter',
#         default=',',
#         help='set CSV delimiter (default ",")')
#     parser.add_option(
#         '-e',
#         '--encoding',
#         default=None,
#         help='character encoding (eg utf-8) to use when reading/writing files (default uses system default)')
#     args, input_files = parser.parse_args()

#     if not input_files:
#         parser.error('What CSV file should be sorted?')
#     elif not args.columns:
#         parser.error('Which columns should be sorted on?')
#     else:
#         # escape backslashes
#         args.delimiter = args.delimiter.decode('string_escape')
#         args.columns = [int(column) if column.isdigit() else column
#                         for column in args.columns]
#         csvsort(
#             input_files[0],
#             columns=args.columns,
#             max_size=args.max_size,
#             has_header=args.has_header,
#             delimiter=args.delimiter,
#             encoding=args.encoding)


# if __name__ == '__main__':
#     main()
