from collections import defaultdict
import datetime
import itertools
import os
import time
import SaveToTxt
import random

#------ Main method to run the code ------
def run(file_name, support_percentage, subset_percentage, main_folder_path, actual_folder_path, num_chunks):
    
    # File Path of data file.
    file_path = f"{os.getcwd()}/{file_name}"

    # code execution date and time
    now = datetime.datetime.now()
    execution_time = now.strftime("%H:%M")
    execution_date = now.strftime("%Y-%m-%d")

    # Append the data with result string for final "_result file".
    result = f'Selected Dataset: {file_name}\n'
    # print the data on console.
    print(f'Selected Dataset: {file_name}')

    # Code Start timer
    code_start_time = time.time()
    
    # read file method timer
    method_start_time = time.time()
    # Count number of file lines/buckets
    with open(file_path, 'r') as data_file:
        # buckets_data = data_file.readlines()
        total_file_lines = 0
        for _ in data_file:
            total_file_lines +=1
    # read file method end timer
    method_end_time = time.time()

    # Append the data with result string for final over "_result file".
    result += f'Read file to count total line function takes {method_end_time - method_start_time:.3f} seconds to execute\n'
    # print the data on console.
    print(f'Read file to count total line function takes {method_end_time - method_start_time:.3f} seconds to execute')

    # Subset calculation code
    total_buckets = total_file_lines
    print("total_buckets: ", total_buckets)
    subset_size = int(total_buckets * (subset_percentage / 100))
        
    result += f'{subset_percentage}% of the entire dataset({total_buckets}): {subset_size}\n'
    print(f'{subset_percentage}% of the entire dataset({total_buckets}): {subset_size}')
    
    # Support Threshold
    support_threshold = int(subset_size * (support_percentage / 100))
    result += f'{support_percentage}% support threshold of the dataset({subset_size}): {support_threshold}\n\n'
    print(f'{support_percentage}% support threshold of the dataset({subset_size}): {support_threshold}')
    
    #----- Chunk end index List ------
    chunk_size = subset_size // num_chunks
    remainder = subset_size % num_chunks
    chunk_end_index = [chunk_size] * num_chunks
    # Distribute the remainder, if any
    for i in range(remainder):
        chunk_end_index[i] += 1

    #-------------------- SON Pass 1 (Single Items) --------------------
    #---------- SON step 1 ----------
    # Divide the input file into num_chunks 
    # (which may be “chunks” in the sense of a distributed file system, or simply a piece of the file). 
    # Treat each chunk as a sample, and run the algorithm of Section 6.4.1(Random sampling) on that chunk.
    # source: Mining of Massive Datasetsbook topic 6.4.3
    def divide_file_into_chunks(start_line, end_line):
        # chunk = buckets_data[start_line : end_line]
        file_chunk = []
        with open(file_path, 'r') as data_file:
            for i, line in enumerate(data_file):
                if start_line <= i < end_line:
                    file_chunk.append(line.strip())
        random.shuffle(file_chunk)
        return file_chunk

    #---------- SON step 2 ----------
    # Use "ps" as the threshold, if each chunk is fraction "p" of the whole file, and "s" is
    # the support threshold. Store on disk all the frequent itemsets found for each chunk.
    # Chunk support threshold
    result += f'Number of chunks: {num_chunks}\n\n'
    print(f'Number of chunks: {num_chunks}')

    chunk_support_threshold = int(support_threshold / num_chunks)
    result += f'Chunk support threshold = total support threshold / num_chunks = {support_threshold}/{num_chunks}: {chunk_support_threshold}\n\n'
    print(f'Chunk support threshold = total support threshold / num_chunks = {support_threshold}/{num_chunks} : {chunk_support_threshold}')
    
    # #---------- SON step 3 ----------
    # # Store on disk all the frequent itemsets found for each chunk
    # read start index
    start = 0
    # read end index
    end = chunk_end_index[0]
    # To reduce the time complexity of the code, 
    # storing the data in the dictionary as items and a set of buckets in which each item appears.
    # This helps in future frequent pair item set count.
    # key: item and value: set of buckets in which each item appears
    item_buckets_data = defaultdict(set)
    # Setting starting bucket number as 1
    bucket_num = 1
    # c1 data dictionary
    all_single_items_data = defaultdict(int)
    # Itemsets that have been found frequent for one or more chunks.
    single_candidate_itemsets = set()
    # C1 method timer
    method_start_time = time.time()
    for i in range(num_chunks):
        # Single_item_chunk for each chunk of file
        # Store on disk all the frequent itemsets found for each chunk.
        single_item_chunk = defaultdict(int)
        chunk_data = divide_file_into_chunks(start, end)
        for bucket in chunk_data:
            for item_str in bucket.strip().split(' '):
                item = int(item_str)
                all_single_items_data[item] += 1
                single_item_chunk[item] += 1
                # This is the main algorithm idea to optimize and speed up the performance of apriori
                # Note: While counting frequent item pairs, with approach required only one for loop than nested for loop.
                # item_buckets_data dictionary is used for frequent pair counting.
                item_buckets_data[item].add(bucket_num)
            bucket_num += 1
        #---------- SON step 4 ----------
        # union of all the itemsets are frequent for one or more chunks
        # if an itemset is not frequent in any chunk, then its support is less than "ps" (total support threshold) in each chunk.
        # However, if any itemset is frequent in any chunk, then we can say it is one of the frequent candidates.
        for key, value in single_item_chunk.items():
            if(value >= chunk_support_threshold):
                single_candidate_itemsets.add(key)
        # Next chunk start and end index.
        if(i < num_chunks - 1):
            start = end
            end += (chunk_end_index[i + 1])

    # Store data store dictionary "item_bucket_data" into text file.
    SaveToTxt.store_item_bucket(f'{actual_folder_path}/item_bucket_data.txt', item_buckets_data, execution_date,'The data is stored in the format of item appearing in each basket. This helps in future frequent item set count.')

    #----------- Kept this code to compare the data with Apriori Algorithm -----------
    # #------ C1 -------
    # single_items = defaultdict(int)
    # for candidate in sorted(single_candidate_itemsets):
    #     single_items[candidate] = all_single_items_data[candidate]
    # SaveToTxt.store_frequent_item(f'{actual_folder_path}/C1.txt', single_items, execution_date,'All items count (C1 data)')
    SaveToTxt.store_candidate_data(f'{actual_folder_path}/C1.txt', single_candidate_itemsets, execution_date,'All candidate(Frequent itemsets found in any chunk)')
    # C1 end time count
    method_end_time = time.time()
    # Append the data with result string for final over "_result file".
    result += f'Pass 1 and Pass 2 function of single items takes {method_end_time - method_start_time:.3f} seconds.\nTotal candidate(Frequent itemsets found in any chunk) itemsets: {len(single_candidate_itemsets)}.\n\n'
    # print the data on console. 
    print(f'Pass 1 and Pass 2 function of single items takes {method_end_time - method_start_time:.3f} seconds.\nTotal candidate(Frequent itemsets found in any chunk) itemsets: {len(single_candidate_itemsets)}.')

    #-------------------- SON Pass 2 (Single Items) --------------------
    #---------- SON step 5 ----------
    # Count all the candidate itemsets and select those that have support at least s as the frequent itemsets.
    #------ L1 -------
    # L1 method timer
    method_start_time = time.time()
    # L1 filter(Prune) data of single_items
    frequent_single_items = defaultdict(int)
    # Note: single_candidate_itemsets are frequent itemset found in any chunk
    for item in sorted(single_candidate_itemsets):
        if all_single_items_data[item] >= support_threshold:
            frequent_single_items[item] = all_single_items_data[item]
    
    # Store single item L1 filter(Prune) data count in "L1" text file.
    SaveToTxt.store_frequent_item(f'{actual_folder_path}/L1.txt', frequent_single_items, execution_date,'Frequent single item count (L1 data)')
    # L1 method end timer
    method_end_time = time.time()
    # Append the data with result string for final "_result file".
    result += f'L1 function takes {method_end_time - method_start_time:.3f} seconds to execute.\nTotal frequent single items count (L1 table length): {len(frequent_single_items)}.\n\n'
    # print the data on console.
    print(f'L1 function takes {method_end_time - method_start_time:.3f} seconds to execute.\nTotal requent single items count (L1 table length): {len(frequent_single_items)}.')
    
    #------ Pair of item set from L1 -------
    method_start_time = time.time()
    # Itertools combinations make a unique pair of L1 (filterd single items) 
    pairs = list(itertools.combinations(sorted(frequent_single_items.keys()), 2))
    method_end_time = time.time()
    # Append the data with result string for final "_result file".
    result += f'Generate pair function takes {method_end_time - method_start_time:.3f} seconds to execute.\nTotal pairs of items from L1: {len(pairs)}\n\n'
    # print the data on console.
    print(f'Generate pair function takes {method_end_time - method_start_time:.3f} seconds to execute.\nTotal pairs of items from L1: {len(pairs)}')
    
    #------ C2 and L2 -------
    # pair_items = defaultdict(int)
    frequent_pair_items = defaultdict(int)
    method_start_time = time.time()
    
    with open(f'{actual_folder_path}/C2.txt','w') as c2_file, open(f'{actual_folder_path}/L2.txt','w') as l2_file:
        c2_file.write(f'Data Execution Date: {execution_date}\n')
        c2_file.write(f'Description: All pair items count (C2 data)\n\n')
        l2_file.write(f'Data Execution Date: {execution_date}\n')
        l2_file.write(f'Description: Frequent pair item count (L2 data)\n\n')
        for pair in pairs:
            pair_count = item_buckets_data[pair[0]].intersection(item_buckets_data[pair[1]])
            if len(pair_count) >= support_threshold:
                frequent_pair_items[pair] = len(pair_count)
                l2_file.write(f'Item: {pair}, Count: {len(pair_count)}\n')
            # pair_items[pair] = len(pair_count)
            c2_file.write(f'Item: {pair}, Count: {len(pair_count)}\n')
    
    method_end_time = time.time()
    # Append the data with result string for final "_result file".
    result += f'C2 and L2 functions take {method_end_time - method_start_time:.3f} seconds to execute.\n'
    # print the data on console.
    print(f'C2 and L2 functions take {method_end_time - method_start_time:.3f} seconds to execute.')
    # Append the data with result string for final "_result file".
    result += f'Total pair items count (C2 table length): {len(pairs)}\n'
    # print the data on console.
    print(f'Total pair items count (C2 table length): {len(pairs)}')
    # Append the data with result string for final "_result file".
    result += f'Total frequent pair items count (L2 table length): {len(frequent_pair_items)}\n\n'
    # print the data on console.
    print(f'Total Frequent pair items count (L2 table length): {len(frequent_pair_items)}')

    #------ Final data store in _result.txt ------- 
    code_end_time = time.time()
    total_time = code_end_time - code_start_time
    if(total_time >= 60):
        result += 'The total time taken to run the entire code is {runTime:.3f} minutes\n'.format(runTime = total_time/60)
        print('The total time taken to run the entire code is {runTime:.3f} minutes'.format(runTime = total_time/60))
    else:
        result += 'The total time taken to run the entire code is {runTime:.3f} seconds\n'.format(runTime = total_time)
        print('The total time taken to run the entire code is {runTime:.3f} seconds'.format(runTime = total_time))
    
    SaveToTxt.store_result(f'{main_folder_path}/result.txt', result, execution_date, execution_time)