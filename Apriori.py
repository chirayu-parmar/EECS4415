from collections import defaultdict
import datetime
import itertools
import os
import time
import SaveToTxt

#------ Main method to run the code ------
def run(file_name, support_percentage, subset_percentage, main_folder_path, actual_folder_path):
    
    # File Path of data file.
    file_path = f"{os.getcwd()}/{file_name}"

    # code execution date and time
    now = datetime.datetime.now()
    execution_time = now.strftime("%H:%M")
    execution_date = now.strftime("%Y-%m-%d")

    # Append the data with result string for final over "_result file".
    result = f'Selected Dataset: {file_name}\n'
    # print the data on the console.
    print(f'Selected Dataset: {file_name}')

    # Code Start timer
    code_start_time = time.time()
    
    # read file method timer
    method_start_time = time.time()
    # Read and store items associated to each bucket in the list.
    # Each list index is the bucket number.  
    with open(file_path, 'r') as data_file:
        buckets_data = data_file.readlines()
    # read file method end timer
    method_end_time = time.time()

    # Append the data with result string for final "_result file".
    result += f'Read file function takes {method_end_time - method_start_time:.3f} seconds to execute.\n'
    # print the data on the console.
    print(f'Read file function takes {method_end_time - method_start_time:.3f} seconds to execute.')

    # Subset calculation code
    total_buckets = len(buckets_data)
    subset_size = int(total_buckets * (subset_percentage / 100))
    
    result += f'{subset_percentage}% of the entire dataset({total_buckets}): {subset_size}\n'
    print(f'{subset_percentage}% of the entire dataset({total_buckets}): {subset_size}')
    
    # Support Threshold
    support_threshold = int(subset_size * (support_percentage / 100))
    result += f'{support_percentage}% support threshold of the dataset({subset_size}): {support_threshold}\n\n'
    print(f'{support_percentage}% support threshold of the dataset({subset_size}): {support_threshold}')
    
    #------------- buckets data based on the subset size -------------
    buckets_data = buckets_data[:subset_size]
    
    #------ C1 -------
    # To reduce the time complexity of the code, 
    # storing the data in the dictionary as items and a set of buckets in which each item appears.
    # This helps in future frequent pair item set count.
    # key: item and value: set of buckets in which each item appears
    item_buckets_data = defaultdict(set)
    # Setting starting bucket number as 1
    bucket_num = 1
    # C1 method timer
    method_start_time = time.time()
    # c1 data dictionary
    single_items = defaultdict(int)
    for bucket in buckets_data:
        for item_str in bucket.strip().split(' '):
            item = int(item_str)
            single_items[item] += 1
            # This is the main algorithm idea to optimize and speed up the performance of apriori
            # Note: While counting frequent item pairs, with approach required only one for loop than nested for loop.
            # item_buckets_data dictionary is used for frequent pair counting.
            item_buckets_data[item].add(bucket_num)
        bucket_num += 1
    
    del buckets_data # remove buckets_data(file data) from the memory.
    #-------- Uncomment the next two lines to check which item appears in which bucket --------
    # Store data store dictionary "item_bucket_data" into text file.
    SaveToTxt.store_item_bucket(f'{actual_folder_path}/item_bucket_data.txt', item_buckets_data, execution_date,'The data is stored in the format of item appearing in each basket. This helps in future frequent item set count.')
    # Store single item count in "C1" text file.
    SaveToTxt.store_frequent_item(f'{actual_folder_path}/C1.txt', single_items, execution_date,'All single items count (C1 data)')
    
    # C1 end time count
    method_end_time = time.time()
    
    # Append the data with result string for final "_result file".
    result += f'C1 function takes {method_end_time - method_start_time:.3f} seconds to execute.\nTotal items count (C1 table length): {len(single_items)}.\n\n'
    # print the data on the console. 
    print(f'C1 function takes {method_end_time - method_start_time:.3f} seconds to execute.\nTotal items count (C1 table length): {len(single_items)}.')
    
    #------ L1 -------
    # L1 method timer
    method_start_time = time.time()
    # L1 filter(Prune) data of single_items
    frequent_single_items = defaultdict(int)
    for key, value in single_items.items():
        if value >= support_threshold:
            frequent_single_items[key] = value

    del single_items # remove C1 from the memory.
    # Store single item L1 filter(Prune) data count in "L1" text file.
    SaveToTxt.store_frequent_item(f'{actual_folder_path}/L1.txt', frequent_single_items, execution_date,'Frequent single items count (L1 data)')
    # L1 method end timer
    method_end_time = time.time()
    # Append the data with result string for final "_result file".
    result += f'L1 function takes {method_end_time - method_start_time:.3f} seconds to execute.\nTotal Frequent single items count (L1 table length): {len(frequent_single_items)}.\n\n'
    # print the data on the console.
    print(f'L1 function takes {method_end_time - method_start_time:.3f} seconds to execute.\nTotal Frequent single items count (L1 table length): {len(frequent_single_items)}.')
    
    #------ Pair of item set from L1 -------
    method_start_time = time.time()
    # Itertools combinations make a unique pairs of L1 (filterd single items) 
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
    # print the data on the console.
    print(f'C2 and L2 functions take {method_end_time - method_start_time:.3f} seconds to execute.')
    # Append the data with result string for final "_result file".
    result += f'Total pair items count (C2 table length): {len(pairs)}\n'
    # print the data on the console.
    print(f'Total pair items count (C2 table length): {len(pairs)}')
    # Append the data with result string for final over "_result file".
    result += f'Total frequent pair items count (L2 table length): {len(frequent_pair_items)}\n\n'
    # print the data on the console.
    print(f'Total frequent pair items count (L2 table length): {len(frequent_pair_items)}')
    
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