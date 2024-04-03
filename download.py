'''
OBJECTIVE OF THIS FILE:-

THIS CODE ITERATES THROUGH ALL YEARS AND FETCHES THEIR URL, SELECTS THE CSV FILES, FETCHES & DOWNLOADS THE CSV FILES AND CREATES A ZIP FILE
THIS CODE HAS AN ADDITIONAL MODE WHICH DETERMINES WHETHER THE WEBSITES ARE RANDOMLY SELECTED FROM ENTIRE DATA OR A SUBSET
INPUT: NCEI Website (Web)
OUTPUT DIR: Archive 
'''

# Importing libraries
import os, requests, time, random, yaml
from bs4 import BeautifulSoup
from urllib.parse import urljoin

class Downloader():# Class for functions required to download files

    def get_size(self, path):
        '''
        Function: To return the size of a file at a given path

        Inputs:-
        path: Location of the file

        Output:-
        Returns the size of the file in bytes if present else -1
        '''
        if os.path.isfile(path): # Checks if the file exists at the location
            return os.path.getsize(path)
        else:
            return -1
        
    def basic_info(self):
        '''
        Function: Provides the basic info of data

        Input:- None

        Output:- 
        main_url[str]: returns the main URL of the website of NCEI i.e. parent directory
        '''
        main_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
        return main_url

    def fetch_URL(self, main_url, year): # Task 1
        '''
        Function: Fetches the URL from the web for a particular year

        Inputs:-
        main_url [str]: Main URL of the NCEI website
        year [int]: Year for which the data needs to be retrieved

        Outputs:-
        response [requests object]: Contains the response of the website
        base_url [str]: URL of the data of particular year
        '''
        YYYY = str(year) + '/'
        base_url = urljoin(main_url, YYYY) # URL for the required year is made
        response = requests.get(base_url) # Response for the website is collected
        if response.status_code == 200: # Status Code 200 indicates that website can be accessed
            print(f"Website for {year} is accessible")
            return response, base_url
        else:
            print(f"Failed to access the website - Status Code: {response.status_code}")
            return -1
        
    def select_files(self, response, year, mode=None, inp_num_files = 100): # Task 2
        '''
        Function:- Selects files for a particular randomly

        Inputs:-
        response [requests object]: Contains the response of the website
        base_url [str]: URL of the data of particular year
        mode [str]: Mode which determines whether the files are selected from entire dataset or a subset for better performance in subsequent stages of the project
        inp_num_files [int]: Number of files to be downloaded

        Outputs:-
        indices [list]: List containing indices of the selected files
        csv_links [list]: List of all csv links obtained by parsing the webpage
        '''
        # The HTML document of webpage of the particular year is parsed
        soup = BeautifulSoup(response.text, 'html.parser')
        # The CSV links are collected in a list from the parsed data
        csv_links = [a['href'] for a in soup.find_all('a', href=lambda href: (href and href.endswith('.csv')))]
        total_num_files = len(csv_links) # Total number of files on the webpage for a particular year
        # The number of files to be selected can be set using inp_num_files
        # This is done to extract a subset of data which can be processed further.
        num_files = min(inp_num_files, total_num_files)
        print(f"No. of files for the year {year} = {total_num_files}")
        indices = []
        if mode == 'specific': # Specific mode ensures that only the files starting with '7' are downloaded as it they are observed to have more amount of monthly data (GT)
            ind_1, ind_2 = 0, 0
            for i in range(total_num_files):
                if csv_links[i][0] == '7' and ind_1 == 0:
                    start = i
                    ind_1 = 1
                if csv_links[i][0] == '8' and ind_2 == 0:
                    end_ = i-1
                    ind_2 = 1
                if ind_1 == 1 and ind_2 == 1:
                    break
            print(f"Start = {start}, end = {end_}")
        else: # This is for selecting files randomly from entire dataset. In specific case too, the data is selected randomly.
            start = 0
            end_ = total_num_files-1
        while len(indices) < num_files:
            # An index is randomly picked from all the files available on the parsed webpage
            idx = random.randint(start, end_) 
            # If the index is already picked earlier, the inner loop ensures that an unique index is picked each time such that none of the files are repeated.
            while idx in indices:
                idx = random.randint(0, total_num_files-1)
            indices.append(idx)
        return indices, csv_links

    def fetch_files(self, directory, indices, csv_links, base_url, year):
        '''
        Function:- To download the selected files and store them in the archive

        Inputs:-
        directory [str]: Name of output directory
        indices [list]: List containing indices of the selected files
        csv_links [list]: List of all csv links obtained by parsing the webpage
        base_url [str]: URL of the data of a particular year
        year [int]: Year for which the files need to be extracted

        Output:- None. Files are stored in the archive
        '''
        folder_size = 0 # Variable for calculating the size of folder of the given year
        print(f"Starting downloading files...\n")
        start = time.time()
        output_directory = os.path.join(directory, str(year)) # Directory for storing the CSV files
        os.makedirs(output_directory, exist_ok=True) # Creates the directory if not existing
        # Iterating through each of the selected files' indices
        for count, idx in enumerate(indices, start=1):
            csv_link = csv_links[idx] # CSV link for current index
            complete_url = urljoin(base_url, csv_link) # Constructing URL for this file
            filename = os.path.basename(complete_url) # Same filename is used
            csv_response = requests.get(complete_url) # Response of the CSV file on web is retrieved
            if csv_response.status_code == 200: # Proceeds if the file is available
                print(f"File no. {count}: {csv_link}  [Index: {idx}] is accessible")
                output_path = os.path.join(output_directory, filename) # Path for the CSV file to be stored
                with open(output_path, 'wb') as csv_file:
                    csv_file.write(csv_response.content) # Writing the CSV data in the file
                print(f"Downloaded: {output_path}")
                file_size = (Downloader().get_size(output_path))/(1024*1024) # Calculating file size in MB
                folder_size += file_size # Updating folder size
                print(f"Size of file: {file_size:.1f} MB")
                print(f"Size of folder {output_directory}: {folder_size:.1f} MB")
                print()
            else:
                print(f"Failed to download: {filename} - Status Code: {csv_response.status_code}")
                break
        end = time.time()
        total_num_files = len(csv_links)
        print(f"Downloaded {count} files out of original {total_num_files} files successfully.")
        print(f"Total time required: {((end-start)/60):.1f} minutes.")

# These two values are set by the year
params = yaml.safe_load(open("params.yaml"))["params"] # Params are loaded from YAML file
year = params["year"] # Year
n_locs = params["n_locs"] # Number of locations to be downloaded
mode = 'specific' # Specific here implies special set of files starting with '7'

# MAIN CODE
downloader = Downloader() # Instance of class
main_url = downloader.basic_info() # Main URL is fetched
main_start = time.time()
output_dir = 'Archive' # Output directory
os.makedirs(output_dir, exist_ok=True) # Output directory is created

print(f"Downloading data for the year {year}")
response, base_url = downloader.fetch_URL(main_url, year) # URL is fetched
indices, csv_links = downloader.select_files(response, year, mode, n_locs) # Files are selected
downloader.fetch_files(output_dir, indices, csv_links, base_url, year) # Files are fetched and stored in a folder
print(f"Downloading data for year {year} completed.\n")
curr_end = time.time()
print(f"Time required till now: {((curr_end-main_start)/60):.0f} minutes.\n")