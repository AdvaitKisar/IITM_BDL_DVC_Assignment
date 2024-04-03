'''
OBJECTIVE OF THIS FILE:-

THIS CODE ITERATES THROUGH ALL CSV FILES AND REFINES THEM I.E. COLLECTS ONLY THE REQUIRED DATA
INPUT DIR: Archive
OUTPUT DIR: Refined
'''

# Importing libraries
import os, yaml
import pandas as pd

class Station_Details():# Class for dealing with station details and related functions
    def __init__(self, year) -> None:
        '''
        Function:- Initializes an object

        Inputs:- 
        self [object]: Instance of the current object
        year [int]: Year

        Output:- None
        '''
        filename = 'Station Details for ' + f'{str(year)}.csv' # Filename of the file containing Station Details
        columns = ['Station Number', 'Latitude', 'Longitude', 'Station Name'] # Columns in the file
        if not os.path.isfile(filename): # Checks for existing file
            df = pd.DataFrame(columns=columns)
            print("Creating new dataframe for Station Details")
        else: # If file exists, imports the station details
            df = pd.read_csv(filename)
            print("Data of Station Details fetched successfully.")
        
        self.df = df
        self.columns = columns
        self.filename = filename

    def store_station_details(self, station_no, lat, long, station_name):
        '''
        Function: Stores the station details in the dataframe

        Inputs:-
        self [object]: Instance of the current object
        station_no [int]: Station Number
        lat [float]: Latitude of the Station
        long [float]: Longitude of the Station
        station_name [str]: Name of the Station

        Output:-
        ind [int]: Indicator which is 1 when one station details are already in the database, else 0.
        '''
        if station_no in list(self.df.loc[:, 'Station Number']):
            print(f"The Station Details of Station No. {station_no} are already in the database")
            ind = 1
        else:
            new_row = pd.DataFrame([[station_no, lat, long, station_name]], columns=self.df.columns)
            self.df = pd.concat([self.df, new_row])
            print(f"Details of Station No. {station_no} added to the dataframe.")
            ind = 0
        return ind

    def save_station_dataframe(self):
        '''
        Function:- Saves the dataframe as csv file

        Inputs:- 
        self [object]: Instance of the current object

        Output:- None. Saves the df as csv file
        '''
        self.df.to_csv(self.filename, header=True, index=False)
        print("Dataframe of Station Details has been stored successfully.")


class RefineData(): # Class for functions used to refine data
    def __init__(self, directory, filename) -> None:
        '''
        Function:- Initializes an object

        Inputs:- 
        self [object]: Instance of the current object
        directory [str]: Directory of the file from where the data has to be retrieved
        filename [str]: CSV File's name for a particular station and year

        Output:- None
        '''
        fields = {
            'Hourly Dew Point Temperature' : 9, # 5
            'Hourly Relative Humidity': 15, # 6
            'Hourly Sea Level Pressure': 17, # 7
            'Hourly Station Pressure': 18, # 8
            'Hourly Wet Bulb Temperature': 20, # 9
            'Monthly Average Rel. Humidity': 46, # 10
            'Monthly Dewpoint Temperature': 59, # 11
            'Monthly Sea Level Pressure': 75, # 12
            'Monthly Station Pressure': 76, # 13
            'Monthly Wet Bulb Temperature': 79 # 14
        } # Dictionary of fields to be extracted from the downloaded data along with column numbers as values and commented numbers as new column indices

        useful_columns = [i for i in range(4)]
        useful_columns.append(5)
        useful_columns.extend(fields.values()) # Indices of useful columns which are to be extracted

        path = os.path.join(directory, filename)
        data = pd.read_csv(path, usecols=useful_columns) # Data from CSV file is fetched
        print(f"The data from {path} has been imported.")
        self.data = data
        self.fields = fields
        self.filename = filename
        self.path = path
        pass

    def replace_date_by_month(self):
        '''
        Function:- Replaces date entries by their month for better computation

        Input:- 
        self [object]: Instance of the current object
        
        Output:- None
        '''
        for i in range(len(self.data['DATE'])):
            s = self.data.loc[i, 'DATE'] # Extracting the string
            month = int(s[5:7]) # Extracting month
            self.data.loc[i, 'DATE'] = month # Setting it in the df
        self.data.rename(columns={'DATE':'MONTH'}, inplace=True) # Renaming the date column

    def save_df_to_csv(self, path):
        '''
        Function:- Saves the refined dataframe of a given station and year as CSV file

        Inputs:- 
        self [object]: Instance of the current object
        path [str]: Path of the folder

        Output:- None
        '''
        output_filename = os.path.join(path, self.filename) # Filename with path
        self.data.to_csv(output_filename, header=True, index=False)
        print(f"Refined CSV File stored at {output_filename}.")

    def check_col(self, col):
        '''
        Function:- Calculates monthly averages for a given parameter

        Inputs:- 
        self [object]: Instance of the current object
        col [int]: Column index of a given parameter e.g. Dew Point Temperature

        Output:-
        Boolean determining whether to proceed with a column or not
        '''
        if self.data.iloc[:, col].notna().any(): # If the entire column has atleast a non-null value, proceed...
            return True
        return False
    
    def check_all_columns(self):
        '''
        Function: Checks all columns of the data for any non-null values

        Input:-
        self [object]: Instance of the current object

        Output:-
        count [int]: Count of columns with atleast one non-null entry
        '''
        columns = self.data.columns[5:] # Columns containing parameters
        count = 0
        for col_no, _ in enumerate(columns, start=5):
            op = self.check_col(col_no)
            if op: # Updates count if the column has atleast single non-null entry
                count += 1
        return count
    
    def save_station_info(self, station_details):
        '''
        Function:- Saves the station details in the dataframe of Station Details

        Inputs:- 
        self [object]: Instance of the current object
        station_details [Station_Details]: Object containing station details

        Output:-
        ind [int]: Indicator which is 1 when one station details are already in the database, else 0.
        '''
        # self.data is the dataframe containing the data from CSV file of a particular station and year
        station_no = self.data.iloc[0, 0]
        station_name = self.data.iloc[0, 4]
        lat, long = self.data.iloc[0, 2], self.data.iloc[0, 3]
        print(f"Station No: {station_no}, Station Name: {station_name}")
        print(f"Lat = {lat:.2f}, Long = {long:.2f}")
        # The station details are saved using the store_station_details function of Station_Details object
        ind = station_details.store_station_details(station_no, lat, long, station_name)
        return ind

# MAIN CODE
params = yaml.safe_load(open("params.yaml"))["params"] # Params are loaded from YAML file
year = params["year"] # Year


station_details = Station_Details(year) # Station Details are imported

main_input_dir = 'Archive' # Input Directory of all years
input_dir = os.path.join(main_input_dir, str(year)) # Input Directory for specific year
main_output_dir = 'Refined' # Output Directory of all years
output_dir = os.path.join(main_output_dir, str(year)) # Output Directory for specific year
os.makedirs(main_output_dir, exist_ok=True)  # Main Output directory is created
os.makedirs(output_dir, exist_ok=True) # Output directory is created

csv_files = [f for f in os.listdir(input_dir) if f.endswith(".csv")] # Names of CSV files are extracted for this year
useful_files_count = 0
for iter, csv_file in enumerate(csv_files): # Iterating through each CSV file
    print(f"Iteration No. {iter+1}: Filename: {csv_file}")
    file_object = RefineData(input_dir, csv_file) # File object for current file
    file_object.replace_date_by_month() # Date is replaced by month
    count = file_object.check_all_columns() # All columns are checked
    if count > 5: # Checks if the files are useful for subsequent analysis
        file_object.save_df_to_csv(output_dir) # Saves the CSV
        file_object.save_station_info(station_details) # Saves the station info
        useful_files_count += 1 # Updates count
    print()
print(f"{useful_files_count} useful files out of {len(csv_files)} files.")
station_details.save_station_dataframe() # Saves station details of all useful stations
print("\n")