from joblib import Parallel, delayed
from thefuzz import fuzz, process
from multiprocessing import Pool


# Dataframe cleaning
def clean(df):
    # Remove columns that end with '_y'
    df = df[df.columns.drop(list(df.filter(regex='_y')))]

    # Rename columns to remove '_x'
    df = df.rename(columns=lambda x: x.replace('_x', ''))
    
    # Drop duplicates in dataframe
    return df.drop_duplicates()


#####################################################################################
# One Attri Matching

# Method 1
##### fuzzy match, add a new col indicating the matching score ##### 

def get_matches(query, choices, score, limit=3):
    results = process.extract(query, choices, scorer=fuzz.token_sort_ratio, limit=limit)
#     return results
    return [result for result in results if result[1] >= score]


# Method 2
##### fuzzy match with specific score, get the number of matches and the matched set #####

def fuzzy_matches(df1, df2, column1, column2, score):
    # Find matches where the fuzzy match score is 100
    matches = df1[column1].apply(lambda x: any(fuzz.ratio(x, y) >= score for y in df2[column2])) 
    
    return df1[matches]


# Method 3
##### fuzzy match with score, new column which includes matching descriptions is added #####
##### The new column contains the matching info of another dataset (matched_value, fuzzy_score, index_in_df2) #####

def fuzzy_matches(df1, df2, column1, column2, score, col_name):
    # Initialize lists to store the matched values and scores
    matched_values = []
    matched_scores = []

    # Iterate over each row in df1
    for x in df1[column1]:
        best_match = None
        best_score = 0

        # Find the best match in df2
        for y in df2[column2]:
            current_score = fuzz.ratio(x, y)
            if current_score > best_score:
                best_score = current_score
                best_match = y

        # Check if the best score meets the threshold
        if best_score >= score:
            match_index = df2[df2[column2] == best_match].index[0]
            matched_values.append((best_match, best_score, match_index))
        else:
            matched_values.append(None)

    # Add the matched values and scores as new columns in df1
    df1[col_name] = matched_values
    
    return df1


# Evaluating the result of 100% fuzzy match 
##### string matches #####

def string_matches(df1, df2, column1, column2):
    # Convert columns to sets for faster comparison
    set2 = set(df2[column2])
    # Find matches where the strings are exactly the same
    matches = df1[column1].apply(lambda x: x in set2)
    return df1[matches]


##################################################################################

##### Multi Attri Matches Method 1 #####
def fuzzy_matches_combined(df1, df2, score, col_name):
    # Create a combined key in both dataframes (exclude YEAR)
    df1['combined'] = df1['conm'] + ',' + df1['add1'] + ',' + df1['addzip'] + ',' + df1['city'] + ',' + df1['state']
    df2['combined'] = df2['ESTAB_NAME'] + ',' + df2['STREET'] + ',' + df2['ZIP'] + ',' + df2['CITY'] + ',' + df2['STATE']

    matched_values = []

    # Iterate over each row in df1
    for x in df1['combined']:
        best_match = None
        best_score = 0

        # Find the best match in df2
        for y in df2['combined']:
            current_score = fuzz.ratio(x, y)
            if current_score > best_score:
                best_score = current_score
                best_match = y

        # Check if the best score meets the threshold
        if best_score >= score:
            match_index = df2[df2['combined'] == best_match].index[0]
            matched_values.append((best_match, best_score, match_index))
        else:
            matched_values.append(None)

    # Add the matched values as a new column in df1
    df1[col_name] = matched_values

    # Drop the combined key column
    df1.drop('combined', axis=1, inplace=True)
    df2.drop('combined', axis=1, inplace=True)

    return df1


##### Multi Attri Matches Method 2 #####
##### Name & Add & Zip & City & State #####

def fuzzy_sep(df1_, df2_):
    fuzzy_name = fuzzy_matches(df1_, df2_, 'conm', 'ESTAB_NAME', 90, 'matched_name')
    fuzzy_add = fuzzy_matches(df1_, df2_, 'add1', 'STREET', 80, 'matched_address') 
    fuzzy_zip = fuzzy_matches(df1_, df2_, 'addzip', 'ZIP', 90, 'matched_zip')
    fuzzy_city = fuzzy_matches(df1_, df2_, 'city', 'CITY', 90, 'matched_city')
    fuzzy_state = fuzzy_matches(df1_, df2_, 'state', 'STATE', 100, 'matched_state')
    # fuzzy_year = fuzzy_matches(df1_, df2_, 'fyear', 'Year', 100, 'matched_year')


    fuzzy_name = fuzzy_name[fuzzy_name['matched_name'].notnull()]
    fuzzy_add = fuzzy_add[fuzzy_add['matched_address'].notnull()]
    fuzzy_zip = fuzzy_zip[fuzzy_zip['matched_zip'].notnull()]
    fuzzy_city = fuzzy_city[fuzzy_city['matched_city'].notnull()]
    fuzzy_state = fuzzy_state[fuzzy_state['matched_state'].notnull()]
    # fuzzy_year = fuzzy_year[fuzzy_year['matched_city'].notnull()]

    # Extract the index of matched names and addresses from the tuples
    fuzzy_name['matched_name_i'] = fuzzy_name['matched_name'].apply(lambda x: x[2]  if x else None)
    fuzzy_add['matched_address_i'] = fuzzy_add['matched_address'].apply(lambda x: x[2] if x else None)
    fuzzy_zip['matched_zip_i'] = fuzzy_zip['matched_zip'].apply(lambda x: x[2] if x else None)
    fuzzy_city['matched_city_i'] = fuzzy_city['matched_city'].apply(lambda x: x[2] if x else None)
    fuzzy_state['matched_state_i'] = fuzzy_state['matched_state'].apply(lambda x: x[2] if x else None)

    intersection = pd.merge(fuzzy_name, fuzzy_add, how='inner', on=['conm', 'add1'])
    intersection = clean(intersection)
    intersection = pd.merge(intersection, fuzzy_zip, how='inner', on=['conm', 'add1', 'addzip'])
    intersection = clean(intersection)
    intersection = pd.merge(intersection, fuzzy_city, how='inner', on=['conm', 'add1', 'addzip', 'city'])
    intersection = clean(intersection)
    intersection = pd.merge(intersection, fuzzy_state, how='inner', on=['conm', 'add1', 'addzip', 'city', 'state'])
    intersection = clean(intersection)
    # intersection = pd.merge(intersection, fuzzy_year, how='inner', on=['conm', 'add1', 'addzip', 'city', 'state', 'fyear'])
    # intersection = clean(intersection)

    # Create a boolean mask where each row is True if all specified columns have the same index value
    # mask = (intersection['matched_name_i'] == intersection['matched_address_i']) & \
    #         (intersection['matched_address_i'] == intersection['matched_zip_i']) & \
    #         (intersection['matched_zip_i'] == intersection['matched_city_i']) & \
    #         (intersection['matched_city_i'] == intersection['matched_state_i'])

    ### It seems that we don't need to match attributes other than the company name and address, 
    # because the fuzzy match procedure only returns the first match, whose index is meaningless.
    mask = (intersection['matched_name_i'] == intersection['matched_address_i'])

    subset_df = intersection[mask]

    print(f'Number of fuzzy matches (Company Name & Add & Zip & City & State & Year): {len(subset_df)}')
    return intersection, subset_df



################################################################################
# Multiprocessing funcs (test

# Function to apply fuzzy matching
def apply_fuzzy_matching(row, choices, scorer=fuzz.token_sort_ratio, limit=1):
    return process.extractOne(str(row), choices, scorer=scorer)

# Function to parallelize fuzzy matching
def parallel_fuzzy_matching(df, column_to_match, choices, num_processes=4):
    with Pool(num_processes) as pool:
        results = pool.starmap(apply_fuzzy_matching, [(row, choices) for row in df[column_to_match]])
    return results