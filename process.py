'''
OBJECTIVE OF THIS FILE:-

THIS CODE ITERATES THROUGH ALL CSV FILES FROM THE REFINED ARCHIVE AND COMPUTES THE MONTHLY AVERAGES FOR ALL PARAMETERS AND STORES IT STATION-WISE.
INPUT DIR: Refined
OUTPUT DIR: Processed
'''

# Importing libraries
import pandas as pd
import numpy as np
import os, re, yaml

class Station_Details():# Class for dealing with station details and related functions
    def __init__(self, year) -> None:
        '''
        Function:- Initializes an object

        Inputs:- 
        self [object]: Instance of the current object

        Output:- None
        '''
        filename = 'Station Details for ' + f'{str(year)}.csv'
        df = pd.read_csv(filename) # Station details are imported
        print("Data of Station Details fetched successfully.")
        self.df = df
    
    def find_useless_files(self):
        '''
        Function:- Finds useless files in the database of Station details. Useless in this context mean the files which do not have any latitude or longitude mentioned

        Inputs:- 
        self [object]: Instance of the current object

        Output:- None
        '''
        n_stations = self.df.shape[0] # Number of unique stations
        useless_files = [] # List to store useless files
        for i in range(n_stations): # Iterating through stations
            if pd.isna(self.df.loc[i, 'Latitude']) == True or pd.isna(self.df.loc[i, 'Longitude']) == True: # Even if one of Latitude or Longitude is absent, such files are considered useless for further downstream tasks
                useless_files.append(self.df.loc[i, 'Station Number'])
        print(f"Useless files = {len(useless_files)}")
        self.useless_files = useless_files

    def check_utility(self, filename):
        '''
        Function:- Checks the utility or usability of a file

        Inputs:- 
        self [object]: Instance of the current object
        filename [str]: Filename of station of the format <STATION_NO>.csv

        Output:-
        ind [int]: Indicator is 1 when file is useful else -1
        '''
        station_code = filename[:-4] # The station code (alphanumeric) is extracted from the filename
        if station_code.isdigit():
            # Numeric station codes are preprocessed as '01234' is saved in the useless_files list as '1234'.
            station_no = str(int(station_code))
        else:
            station_no = station_code
        if station_no in self.useless_files: # Checks if the file is in list of useless files
            ind = -1
            print(station_no)
        else:
            ind = 1
        return ind


class Monthly_Average_Calculator(): # Class for calculating montly averages and storing them
    def __init__(self, directory, filename) -> None:
        '''
        Function:- Initializes an object

        Inputs:- 
        self [object]: Instance of the current object
        directory [str]: Directory where refined data is stored
        filename [str]: Filename of the form <STATION_NO>.csv

        Output:- None
        '''
        col_renames = {
            'HourlyDewPointTemperature': 'Computed Dew Point Temperature',
            'HourlyRelativeHumidity': 'Computed Relative Humidity',
            'HourlySeaLevelPressure': 'Computed Sea Level Pressure',
            'HourlyStationPressure': 'Computed Station Pressure',
            'HourlyWetBulbTemperature': 'Computed Wet Bulb Temperature',
            'MonthlyAverageRH': 'GT Relative Humidity',
            'MonthlyDewpointTemperature': 'GT Dew Point Temperature',
            'MonthlySeaLevelPressure': 'GT Sea Level Pressure',
            'MonthlyStationPressure': 'GT Station Pressure',
            'MonthlyWetBulb': 'GT Wet Bulb Temperature'
        } # New renames for the new df containing averages
        # Instead of getting output from the prepare.py as list of fields, the fields which were useful were predefined by observing the column names
        path = os.path.join(directory, filename) # Path is constructed
        data = pd.read_csv(path) # Refined Archive's Data for given filename is fetched
        print(f"The data from {path} has been imported.")
        self.data = data
        self.filename = filename
        self.col_renames = col_renames
        pass

    def convert_and_extract(self, row, col):
        '''
        Function:- Extracts and converts a given cell value to float. This function was useful for extracting numerical values from mistyped strings like '32s', '-41.43a', etc.

        Inputs:- 
        self [object]: Instance of the current object
        row [int]: Row index
        col [int]: Column index

        Output:- 
        Extracted and converted float value
        '''
        var = self.data.iloc[row, col]
        try:
            return float(var)
        except ValueError:
            pass
        try:
            match = re.search(r"(\-?\d+\.?\d*)", var)
            if match:
                return float(match.group(1))
        except ValueError:
            pass
        return None

    def calculate_monthly_average_per_param(self, col):
        '''
        Function:- Calculates monthly averages for a given parameter

        Inputs:- 
        self [object]: Instance of the current object
        col [int]: Column index of a given parameter e.g. Dew Point Temperature

        Output:-
        average_dict [dict]: Dictionary containing the average values of the parameter as the values of the dictionary and keys are the month numbers (1 - Jan, 2 - Feb, etc.)
        '''
        counter_dict = {i: 0 for i in range(1, 13)} # Dictionary for counting the number of entries for each month
        param_dict = {i: 0 for i in range(1, 13)} # Dictionary for summing up the entries for each month
        n_samples = self.data.shape[0] # Number of entries/samples
        if self.data.iloc[:, col].notna().any(): # If the entire column has atleast a non-null value, proceed...
            for i in range(n_samples): # Iterating through the entries
                # If the entry is not a null value and if it is convertible to float, then proceed...
                if pd.notna(self.data.iloc[i, col]) and self.convert_and_extract(i, col) != None:
                    month = int(self.data.iloc[i, 1]) # Extracting month
                    param_dict[month] += self.convert_and_extract(i, col) # Summing up the parameter's entry to respective month
                    counter_dict[month] += 1 # Updating count
            average_dict = {i: 0 for i in range(1, 13)} # Dictionary containing average values
            for key in average_dict.keys():
                if counter_dict[key] == 0: # If count is 0, then the average is None
                    print(f"Count for {key}th month for {col}th column = 0")
                    avg = None
                else:
                    avg = param_dict[key]/counter_dict[key]
                average_dict[key] = avg
            return average_dict
        return None
    
    def calculate_MA_for_all_params(self, output_directory):
        '''
        Function:- Calculates monthly averages for all parameters

        Inputs:- 
        self [object]: Instance of the current object
        output_directory [str]: Output Directory

        Output:-
        None
        '''
        original_renames = list(self.col_renames.values())
        columns = ['MONTH']
        columns.extend(original_renames[:5]) # Columns for a station
        n_params = 5 # Number of parameters
        MA_array = np.zeros((12, n_params+1)) # 2D Array for storing monthly averages
        for i in range(12):
            MA_array[i, 0] = i+1 # Storing month numbers in 1st column
        for col in range(5, n_params+5): # Iterating through each parameter
            op = self.calculate_monthly_average_per_param(col) # Output from function calculating the monthly averages for a single parameter
            if op:
                average_dict = op
                for month, avg in average_dict.items():
                    MA_array[month-1, col-4] = avg # Averages are saved in appropriate cells
        print(f"Calculated all monthly averages for the file {self.filename}")
        data_MA = pd.DataFrame(MA_array, columns=columns) # Pandas dataframe is created
        path = os.path.join(output_directory, self.filename)
        data_MA.to_csv(path, index=False) # Monthly averages for given station have been saved to a CSV file as <STATION_NO>.csv in the diretory 'Monthly Averages'
        print(f"Saved monthly averages at {path}.")

# MAIN CODE
params = yaml.safe_load(open("params.yaml"))["params"] # Params are loaded from YAML file
year = params["year"] # Year


main_input_dir = 'Refined' # Input Directory of all years
input_dir = os.path.join(main_input_dir, str(year)) # Input Directory for specific year
main_output_dir = 'Processed' # Output Directory of all years
output_dir = os.path.join(main_output_dir, str(year)) # Output Directory for specific year
os.makedirs(main_output_dir, exist_ok=True) # Main Output directory is created
os.makedirs(output_dir, exist_ok=True) # Output directory is created

station_details = Station_Details(year) # Station details are retrieved
station_details.find_useless_files() # Useless files are found
print()

csv_files = [f for f in os.listdir(input_dir) if f.endswith(".csv")] # CSV filenames are listed
for iter, file in enumerate(csv_files, start=1): # Iterating through each filename
    print(f"Processing File No. {iter}: {file}")
    if station_details.check_utility(file) == -1: # Checking for usefulness of file in terms of presence of latitude and longitude
        print(f"File No. {iter}: {file} is useless.")
        continue
    file_object = Monthly_Average_Calculator(input_dir, file) # File object is created
    file_object.calculate_MA_for_all_params(output_dir) # Monthly averages are calculated and stored
    print()