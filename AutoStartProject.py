from collections import defaultdict
import os
import sys
import Apriori
import PCY
import RandomSampling
import SON
import Multihash

def run (algorithm, data, support, subset, chunk):
    #----------- Actual Code ----------
    print('\n-------------------------------------------')
    print(f'Selected algorithm: {algorithm}.')
    # Selected algorithm
    selected_algorithm = algorithm

    print(f'Selected file: {data}.')
    file_name = data

    # support
    print(f'Enter Support Percentage: {support}')
    support_percentage = int(support)
    # Data Subsets
    print(f'Enter Data Subsets Percentage: {subset}')
    subset_percentage = int(subset)
    # File Path of data file.
    file_path = f"{os.getcwd()}/{file_name}"

    # ------ Code creates a Necessary folders and subfolders ------
    # codition to check file exists or not.
    if not os.path.exists(file_path):
        print("file " + file_name + " does not exist !!")
        # Exit the code
        sys.exit()

    # Create a root folder called Apriori in working directory.
    root_folder = f'{os.getcwd()}/{selected_algorithm}'
    if not os.path.exists(root_folder):
        os.mkdir(root_folder)

    # Main folder name.
    folder_name = file_name.split(".")[0]
    # Create a main folder where all respective data file be stored.
    main_folder_path = f'{os.getcwd()}/{selected_algorithm}/{folder_name}'
    # Codition to check folder exists or not.
    if not os.path.exists(main_folder_path):
        os.mkdir(main_folder_path)

    actual_folder_path = f'{os.getcwd()}/{selected_algorithm}/{folder_name}/{folder_name}_sup{support_percentage}%_subset{subset_percentage}'
    if not os.path.exists(actual_folder_path):
        os.mkdir(actual_folder_path)

    if(selected_algorithm == "Apriori"): Apriori.run(file_name, support_percentage, subset_percentage, main_folder_path, actual_folder_path)
    elif(selected_algorithm == "PCY"): PCY.run(file_name, support_percentage, subset_percentage, main_folder_path, actual_folder_path)
    elif(selected_algorithm == "Random Sampling"): RandomSampling.run(file_name, support_percentage, subset_percentage, main_folder_path, actual_folder_path)
    elif(selected_algorithm == "SON"):
        num_chunks = int(chunk)
        print(f"Enter read chunk size: {num_chunks}")
        SON.run(file_name, support_percentage, subset_percentage, main_folder_path, actual_folder_path, num_chunks)
    elif(selected_algorithm == "Multihash"): Multihash.run(file_name, support_percentage, subset_percentage, main_folder_path, actual_folder_path)
    else: print("Invalid choice. Please enter a number between 1 and 5.") 
    
    print('-------------------------------------------\n')

# run (algorithm, data, support, subset, chunk)
# #------- Apriori -------
# # ------- retail.dat -------
# run("Apriori", "retail.dat", 1, 20, "N/A")
# run("Apriori", "retail.dat", 1, 40, "N/A")
# run("Apriori", "retail.dat", 1, 60, "N/A")
# run("Apriori", "retail.dat", 1, 80, "N/A")
# run("Apriori", "retail.dat", 1, 100, "N/A")
# #------- netflix.data -------
# run("Apriori", "netflix.data", 1, 20, "N/A")
# run("Apriori", "netflix.data", 1, 40, "N/A")
# run("Apriori", "netflix.data", 1, 60, "N/A")
# run("Apriori", "netflix.data", 1, 80, "N/A")
# run("Apriori", "netflix.data", 1, 100, "N/A")

# #------- Apriori -------
# #------- retail.dat -------
# run("Apriori", "retail.dat", 2, 20, "N/A")
# run("Apriori", "retail.dat", 2, 40, "N/A")
# run("Apriori", "retail.dat", 2, 60, "N/A")
# run("Apriori", "retail.dat", 2, 80, "N/A")
# run("Apriori", "retail.dat", 2, 100, "N/A")
# #------- netflix.data -------
# run("Apriori", "netflix.data", 2, 20, "N/A")
# run("Apriori", "netflix.data", 2, 40, "N/A")
# run("Apriori", "netflix.data", 2, 60, "N/A")
# run("Apriori", "netflix.data", 2, 80, "N/A")
# run("Apriori", "netflix.data", 2, 100, "N/A")

# #------- PCY -------
# #------- retail.dat -------
# run("PCY", "retail.dat", 1, 20, "N/A")
# run("PCY", "retail.dat", 1, 40, "N/A")
# run("PCY", "retail.dat", 1, 60, "N/A")
# run("PCY", "retail.dat", 1, 80, "N/A")
# run("PCY", "retail.dat", 1, 100, "N/A")
# #------- retail.dat -------
# run("PCY", "retail.dat", 2, 20, "N/A")
# run("PCY", "retail.dat", 2, 40, "N/A")
# run("PCY", "retail.dat", 2, 60, "N/A")
# run("PCY", "retail.dat", 2, 80, "N/A")
# run("PCY", "retail.dat", 2, 100, "N/A")

# #------- netflix.data -------
# run("PCY", "netflix.data", 1, 20, "N/A")
# run("PCY", "netflix.data", 1, 40, "N/A")
# run("PCY", "netflix.data", 1, 60, "N/A")
# run("PCY", "netflix.data", 1, 80, "N/A")
# run("PCY", "netflix.data", 1, 100, "N/A")
# #------- netflix.data -------
# run("PCY", "netflix.data", 2, 20, "N/A")
# run("PCY", "netflix.data", 2, 40, "N/A")
# run("PCY", "netflix.data", 2, 60, "N/A")
# run("PCY", "netflix.data", 2, 80, "N/A")
# run("PCY", "netflix.data", 2, 100, "N/A")

# run (algorithm, data, support, subset, chunk)
# ------- Random Sampling -------
# ------- retail.dat -------
# run("RandomSampling", "retail.dat", 1, 20, "N/A")
# run("RandomSampling", "retail.dat", 1, 40, "N/A")
# run("RandomSampling", "retail.dat", 1, 60, "N/A")
# run("RandomSampling", "retail.dat", 1, 80, "N/A")
# run("RandomSampling", "retail.dat", 1, 100, "N/A")
# #------- netflix.data -------
# run("RandomSampling", "netflix.data", 1, 20, "N/A")
# run("RandomSampling", "netflix.data", 1, 40, "N/A")
# run("RandomSampling", "netflix.data", 1, 60, "N/A")
# run("RandomSampling", "netflix.data", 1, 80, "N/A")
# run("RandomSampling", "netflix.data", 1, 100, "N/A")

# #------- Random Sampling -------
# #------- retail.dat -------
# run("RandomSampling", "retail.dat", 2, 20, "N/A")
# run("RandomSampling", "retail.dat", 2, 40, "N/A")
# run("RandomSampling", "retail.dat", 2, 60, "N/A")
# run("RandomSampling", "retail.dat", 2, 80, "N/A")
# run("RandomSampling", "retail.dat", 2, 100, "N/A")
# #------- netflix.data -------
# run("RandomSampling", "netflix.data", 2, 20, "N/A")
# run("RandomSampling", "netflix.data", 2, 40, "N/A")
# run("RandomSampling", "netflix.data", 2, 60, "N/A")
# run("RandomSampling", "netflix.data", 2, 80, "N/A")
# run("RandomSampling", "netflix.data", 2, 100, "N/A")

# #------- SON -------
# #------- retail.dat -------
# run("SON", "retail.dat", 1, 20, 1)
# run("SON", "retail.dat", 1, 40, 2)
# run("SON", "retail.dat", 1, 60, 3)
# run("SON", "retail.dat", 1, 80, 4)
# run("SON", "retail.dat", 1, 100, 5)
# #------- netflix.data -------
# run("SON", "netflix.data", 1, 20, 2)
# run("SON", "netflix.data", 1, 40, 4)
# run("SON", "netflix.data", 1, 60, 6)
# run("SON", "netflix.data", 1, 80, 8)
# run("SON", "netflix.data", 1, 100, 10)

# #------- retail.dat -------
# run("SON", "retail.dat", 2, 20, 1)
# run("SON", "retail.dat", 2, 40, 2)
# run("SON", "retail.dat", 2, 60, 3)
# run("SON", "retail.dat", 2, 80, 4)
# run("SON", "retail.dat", 2, 100, 5)
# #------- netflix.data -------
# run("SON", "netflix.data", 2, 20, 2)
# run("SON", "netflix.data", 2, 40, 4)
# run("SON", "netflix.data", 2, 60, 6)
# run("SON", "netflix.data", 2, 80, 8)
# run("SON", "netflix.data", 2, 100, 10)


# #------- Multihash -------
# #------- retail.dat -------
# run("Multihash", "retail.dat", 1, 20, "N/A")
# run("Multihash", "retail.dat", 1, 40, "N/A")
# run("Multihash", "retail.dat", 1, 60, "N/A")
# run("Multihash", "retail.dat", 1, 80, "N/A")
# run("Multihash", "retail.dat", 1, 100, "N/A")
# # #------- retail.dat -------
# run("Multihash", "retail.dat", 2, 20, "N/A")
# run("Multihash", "retail.dat", 2, 40, "N/A")
# run("Multihash", "retail.dat", 2, 60, "N/A")
# run("Multihash", "retail.dat", 2, 80, "N/A")
# run("Multihash", "retail.dat", 2, 100, "N/A")

# #------- netflix.data -------
# run("Multihash", "netflix.data", 1, 20, "N/A")
# run("Multihash", "netflix.data", 1, 40, "N/A")
# run("Multihash", "netflix.data", 1, 60, "N/A")
# run("Multihash", "netflix.data", 1, 80, "N/A")
# run("Multihash", "netflix.data", 1, 100, "N/A")
# #------- netflix.data -------
# run("Multihash", "netflix.data", 2, 20, "N/A")
# run("Multihash", "netflix.data", 2, 40, "N/A")
# run("Multihash", "netflix.data", 2, 60, "N/A")
# run("Multihash", "netflix.data", 2, 80, "N/A")
# run("Multihash", "netflix.data", 2, 100, "N/A")

# run (algorithm, data, support, subset, chunk)
#------- Apriori -------
# ------- retail.dat -------
run("Apriori", "retail.dat", 1, 100, "N/A")
# ------- netflix.data -------
# run("Apriori", "netflix.data", 1, 100, "N/A")

#------- PCY -------
# ------- retail.dat -------
run("PCY", "retail.dat", 1, 100, "N/A")
#------- netflix.data -------
# run("PCY", "netflix.data", 1, 100, "N/A")

#------- Random Sampling -------
# ------- retail.dat -------
run("Random Sampling", "retail.dat", 1, 100, "N/A")
#------- netflix.data -------
# run("Random Sampling", "netflix.data", 1, 100, "N/A")

#------- SON -------
# ------- retail.dat -------
run("SON", "retail.dat", 1, 100, 4)
#------- netflix.data -------
# run("SON", "netflix.data", 1, 100, 4)

#------- MultiHash -------
# ------- retail.dat -------
run("Multihash", "retail.dat", 1, 100, "N/A")
#------- netflix.data -------
# run("Multihash", "netflix.data", 1, 100, "N/A")


