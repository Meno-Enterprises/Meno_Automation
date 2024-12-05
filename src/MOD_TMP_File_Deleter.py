#!/usr/bin/env python3
"""
Aria Corona, Dec 5th 2024
This script removes all files in a specified directory and its subdirectories,
except for files with the '.ppd' extension. It does not remove any folders.
Functions:
    remove_files_but_not_folders(directory):
        Removes all files in the given directory and its subdirectories,
        except for files with the '.ppd' extension.
Usage:
    Run the script to remove files in the specified directory.
    The directory to be processed is defined by the DIRECTORY constant.
"""

import os

DIRECTORY = 'Z:/Test_Hotfolders'

def remove_files_but_not_folders(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.ppd'):
                continue
            file_path = os.path.join(root, file)
            os.remove(file_path)
            print(f"Removed file: {file_path}")

if __name__ == "__main__":
    remove_files_but_not_folders(DIRECTORY)