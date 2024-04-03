'''
OBJECTIVE OF THIS FILE:-

THIS CODE ITERATES THROUGH ALL CSV FILES FROM THE PROCESSED ARCHIVE AND CONSOLIDATES ENTIRE DATA AND FINDS THE R2 SCORE OF COMPUTED AND GROUND TRUTH VALUES OF MONTHLY AVERAGES
INPUT DIR: Prepared
OUTPUT DIR: Consolidated
'''
# Importing libraries
import os, yaml
import pandas as pd
from sklearn.metrics import r2_score
from dvclive import Live

class Experiment_Records(): # Class for recording experimental data
    def __init__(self) -> None:
        '''
        Function: Initializes the class object and imports/creates dataframe for recording the r2 score
        '''
        filename = 'Experiment Records.csv' # FIlename
        if not os.path.isfile(filename):
            columns = ['Year', 'R2 Score']
            df = pd.DataFrame(columns=columns)
        else:
            df = pd.read_csv(filename)
        self.df = df
        self.filename = filename

    def record(self, year, score):
        '''
        Function: Records the current experiment's year and score and saves it in the spreadsheet

        Inputs:-
        self [object]: Instance of the current object
        year [int]: Year
        score [float]: R2 score

        Output: None
        '''
        self.df.loc[len(self.df.index)] = [year, score]
        self.df.to_csv(self.filename, index=False)
        

class DataConsolidator(): # Class for functions used to consolidate data and evaluate R2 score
    def __init__(self, directory, year) -> None:
        '''
        Function:- Initializes an object

        Inputs:- 
        self [object]: Instance of the current object

        Output:- None
        '''
        filename = 'Consolidated Data of ' + f'{str(year)}.csv' # Filename of the file containing particular year's consolidated data
        path = os.path.join(directory, filename)
        columns = ['File No.', 'Parameter', 'Computed', 'Ground Truth'] # Columns in the file
        df = pd.DataFrame(columns=columns)
        print("Creating new dataframe for Main Consolidated Data")

        related_cols = {
            1: 7,
            2: 6,
            3: 8,
            4: 9,
            5: 10
        } # Related columns are used for evaluation. Eg. Computed Sea Level Pressure (Monthly Average) using Hourly Data and Ground Truth Sea Level Pressure (Monthly Data from website)
        self.df = df
        self.columns = columns
        self.filename = filename
        self.related_cols = related_cols
        self.path = path
        self.year = year

    def extract_useful_data(self, directory, filename):
        '''
        Function:- Extracts the data from the given file which has both pairs of data i.e. ground truth and computed value

        Inputs:- 
        self [object]: Instance of the current object
        directory [str]: Directory in which file is stored
        filename [str]: Filename of file containing monthly averages and ground truths of a given station

        Output:- None
        '''
        path = os.path.join(directory, filename)
        data = pd.read_csv(path) # Data is imported
        data = data.fillna(0) # Null values are substituted by 0 for easy handling
        hourly_cols = list(self.related_cols.keys()) # List of columns which were computed using hourly data
        for col in hourly_cols: # Iterating through each column
            # Checks if either of the columns of ground truth and computed values are entirely filled with zeros
            if (data.iloc[:, col] == 0).all() == True or (data.iloc[:, self.related_cols[col]] == 0).all() == True:
                continue # Skips such columns
            col_name = data.columns[col] # Column name
            param = ' '.join(col_name.split(' ')[1:]) # Parameter name
            for i in range(len(data.iloc[:, col])): # Iterating through each month for given pair of columns
                computed = float(data.iloc[i, col]) # Computed value
                ground_truth = float(data.iloc[i, self.related_cols[col]]) # Ground truth value
                if computed != 0 and ground_truth != 0: # If both are non-zero, then their stored
                    self.df.loc[len(self.df.index)] = [filename[:-4], param, computed, ground_truth]

    def save_consolidated_data(self):
        '''
        Function:- Stores the consolidated data

        Inputs:- 
        self [object]: Instance of the current object

        Output:- None
        '''
        self.df.to_csv(self.path, index=False) # Stores as CSV file
        print(f"Saved all data successfully at {self.path}.")

    def compute_r2_score(self, live):
        '''
        Function:- Computes the R2 Score and determines consistency of dataset and also records it

        Inputs:- 
        self [object]: Instance of the current object

        Output:- None
        '''
        computed_col = list(self.df['Computed']) # Computed column is extacted as list from consolidated data
        ground_truth_col = list(self.df['Ground Truth']) # Ground truth column is extacted as list from consolidated data
        score = r2_score(ground_truth_col, computed_col) # R2 score is computed
        print(f"R2 Score for the year {self.year} is {score:.4f}.", end=' ')
        if score >= 0.9: # Threshold for consistency is 0.9
            print("This dataset is consistent.")
        else:
            print("This dataset is not consistent.")
        if not live.summary:
            live.summary = {"r2_score": {}}
        live.summary["r2_score"][year] = score
        Experiment_Records().record(self.year, score) # Record of this score is saved

# MAIN CODE
params = yaml.safe_load(open("params.yaml"))["params"] # Params are loaded from YAML file
year = params["year"] # Year


main_input_dir = 'Prepared' # Input Directory of all years
input_dir = os.path.join(main_input_dir, str(year)) # Input Directory for specific year
main_output_dir = 'Consolidated' # Output Directory of all years
output_dir = os.path.join(main_output_dir, str(year)) # Output Directory for specific year
os.makedirs(main_output_dir, exist_ok=True) # Main Output directory is created
os.makedirs(output_dir, exist_ok=True) # Output directory is created

data_consolidator = DataConsolidator(output_dir, year) # Data consolidator object is generated
csv_files = [f for f in os.listdir(input_dir) if f.endswith(".csv")] # CSV filenames are listed
for iter, file in enumerate(csv_files, start=1): # Iterating through each filename
    print(f"Processing File No. {iter}: {file}")
    data_consolidator.extract_useful_data(input_dir, file) # Extracts the useful data
    print()
data_consolidator.save_consolidated_data() # Saves the consolidated data
EVAL_PATH = "eval"
os.makedirs(EVAL_PATH, exist_ok=True)
with Live(EVAL_PATH, dvcyaml=False) as live:
    data_consolidator.compute_r2_score(live) # Finds R2 Score