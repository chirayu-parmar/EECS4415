from collections import defaultdict
import datetime
import os
import sys
import Apriori
import PCY
import RandomSampling
import SON
import Multihash

#----------- Actual Code ----------
algorithms = {1: "Apriori", 2: "PCY", 3: "Random Sampling", 4: "SON", 5: "Multihash"}

print("\nEnter '1' for Apriori")
print("Enter '2' for PCY")
print("Enter '3' for Random Sampling")
print("Enter '4' for SON")
print("Enter '5' for Multihash\n")
algorithm_choice = int(input("Enter your choice (1-5): "))
# Selected algorithm
selected_algorithm = algorithms[algorithm_choice]

files = {1: "retail.dat", 2: "netflix.data"}
print("\nEnter '1' for retail.dat")
print("Enter '2' for netflix.data\n")
file_choice = int(input("Enter your choice (1 or 2): "))
# File name
file_name = files[file_choice]

# support
support_percentage = int(input("Enter Support Percentage: "))
# Data Subsets
subset_percentage = int(input("Enter Data Subsets Percentage: "))
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

if(algorithm_choice == 1): Apriori.run(file_name, support_percentage, subset_percentage, main_folder_path, actual_folder_path)
elif(algorithm_choice == 2): PCY.run(file_name, support_percentage, subset_percentage, main_folder_path, actual_folder_path)
elif(algorithm_choice == 3): RandomSampling.run(file_name, support_percentage, subset_percentage, main_folder_path, actual_folder_path)
elif(algorithm_choice == 4):
    num_chunks = int(input("Enter read chunk size: "))
    SON.run(file_name, support_percentage, subset_percentage, main_folder_path, actual_folder_path, num_chunks)
elif(algorithm_choice == 5): Multihash.run(file_name, support_percentage, subset_percentage, main_folder_path, actual_folder_path)
else: print("Invalid choice. Please enter a number between 1 and 5.") 