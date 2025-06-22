# Store the data in text file.
# The data is stored in the format of Buckets and Items.
# Same as the actual file.
def store_bucket_item(file_name, data, execution_date, description):
    with open(file_name, 'w') as file:
        file.write(f'Data Execution Date: {execution_date}\n')
        file.write(f'Description: {description}\n\n')
        for bucket, items in sorted(data.items()):
            file.write(f'Bucket: {bucket}, Items: {sorted(items)}\n')

# Store the data in text file.
# To reduce the time complexity of the code storing the data in item appearing in each basket.
# This helps in frequent item set count.  
def store_item_bucket(file_name, data, execution_date, description):
    with open(file_name, 'w') as file:
        file.write(f'Data Execution Date: {execution_date}\n')
        file.write(f'Description: {description}\n\n')
        for item, buckets in sorted(data.items()):
            file.write(f'Item: {item}, Buckets: {sorted(buckets)}\n')

# Store the frequent data items in text file.
def store_frequent_item(file_name, data, execution_date, description):
    with open(file_name, 'w') as file:
        file.write(f'Data Execution Date: {execution_date}\n')
        file.write(f'Description: {description}\n\n')
        for item, count in sorted(data.items()):
            file.write(f'Item: {item}, Count: {count}\n')

# Store the PCY hash table data in text file.
def store_hash_data(file_name, data, execution_date, description):
    with open(file_name, 'w') as file:
        file.write(f'Data Execution Date: {execution_date}\n')
        file.write(f'Description: {description}\n\n')
        for hash_value, count in sorted(data.items()):
            file.write(f'Hash_Value: {hash_value}, Count: {count}\n')

# Store the SON candidate items in text file.
def store_candidate_data(file_name, data, execution_date, description):
    with open(file_name, 'w') as file:
        file.write(f'Data Execution Date: {execution_date}\n')
        file.write(f'Description: {description}\n\n')
        for item in sorted(data):
            file.write(f'Item: {item}\n')

# Store final result.
# This .txt file contains the overview of the data. 
# Such as how much time it took to run method, how many data are stroed in each dictionary.
def store_result(file_name, data, execution_date, execution_time):
    with open(file_name, 'a+') as file:
        file.write(f'-----------------------------------------------\n')
        file.write(f'Data Execution Time: {execution_time} on {execution_date}\n\n')
        file.write(data)