from parallelpixie.pixie import Pixie
import time
import warnings
warnings.filterwarnings('ignore',category=FutureWarning)

from multiprocessing import Pool
import multiprocessing
import pandas as pd
import numpy as np

def process_replace(chunk):
    replacements = {
        'USMER': {2:0},
        'SEX': {2: 0},
        'PATIENT_TYPE': {2: 0},
        'INTUBED': {2: 0},
        'PNEUMONIA': {2: 0},
        'PREGNANT': {2: 0},
        'DIABETES': {2: 0, 98: 99},
        'COPD': {2: 0, 98: 99},
        'ASTHMA': {2: 0, 98: 99},
        'INMSUPR': {2: 0, 98: 99},
        'HIPERTENSION': {2: 0, 98: 99},
        'CARDIOVASCULAR': {2: 0, 98: 99},
        'OBESITY': {2: 0, 98: 99},
        'RENAL_CHRONIC': {2: 0, 98: 99},
        'TOBACCO': {2: 0, 98: 99},
        'CLASIFFICATION_FINAL': {4: 0, 5: 0, 6: 0, 7: 0, 2: 1, 3: 1},
        'ICU': {2: 0, 97: 0},
    }
    return chunk.replace(replacements)

# Define the processing function for a single chunk
def process_chunk(chunk):
    # Replicate the dataset 10 times
    chunk = pd.concat([chunk] * 30, ignore_index=True)

    # Perform typecasting
    chunk['USMER'] = chunk['USMER'].astype(int)
    chunk['SEX'] = chunk['SEX'].astype(int)
    chunk['PATIENT_TYPE'] = chunk['PATIENT_TYPE'].astype(int)
    chunk['INTUBED'] = chunk['INTUBED'].astype(int)
    chunk['PNEUMONIA'] = chunk['PNEUMONIA'].astype(int)
    chunk['PREGNANT'] = chunk['PREGNANT'].astype(int)
    chunk['DIABETES'] = chunk['DIABETES'].astype(int)
    chunk['COPD'] = chunk['COPD'].astype(int)
    chunk['ASTHMA'] = chunk['ASTHMA'].astype(int)
    chunk['HIPERTENSION'] = chunk['HIPERTENSION'].astype(int)
    chunk['ICU'] = chunk['ICU'].astype(int)
    chunk['INMSUPR'] = chunk['INMSUPR'].astype(int)
    chunk['CARDIOVASCULAR'] = chunk['CARDIOVASCULAR'].astype(int)
    chunk['TOBACCO'] = chunk['TOBACCO'].astype(int)
    chunk['RENAL_CHRONIC'] = chunk['RENAL_CHRONIC'].astype(int)
    chunk['CLASIFFICATION_FINAL'] = chunk['CLASIFFICATION_FINAL'].astype(int)

    # Perform replacements
    chunk = process_replace(chunk)

    # Process DATE_DIED
    chunk['AGE'] = chunk['AGE'].astype(int)
    chunk['DATE_DIED'] = chunk['DATE_DIED'].replace('9999-99-99', np.nan)
    chunk['DATE_DIED'] = pd.to_datetime(chunk['DATE_DIED'], format='%d/%m/%Y', errors='coerce')
    chunk['DATE_DIED'] = chunk['DATE_DIED'].apply(lambda x: 1 if pd.notnull(x) else 0).astype(int)

    return chunk


# Parallel processing function
def preprocess_parallel(file_path, chunk_size):
    temp = Pixie.from_csv(file_path, chunk_size)  # Assume Pixie provides chunks
    chunks = list(temp.data_source)  # Convert to a list of chunks for parallelization

    # Use multiprocessing to process chunks in parallel
    with Pool() as pool:
        processed_chunks = pool.map(process_chunk, chunks)

    # Concatenate all processed chunks into a single DataFrame
    return pd.concat(processed_chunks, ignore_index=True)


if __name__ == '__main__':
    # file_path='../covid-data.csv'
    # start_time_load = time.time()
    # processed_df = preprocess_parallel(file_path, 82000)
    # end_time_load = time.time()
    # load_execution_time = end_time_load - start_time_load
    # print(f"parallel preprocessing execution time:{load_execution_time}")
    # #print(processed_df.head())


    file_path = '../covid-data.csv'
    #processed_df = preprocess_parallel(file_path, 82000)
    num_runs = 1  # Number of times to run the function
    execution_times = []
    #print (f"cup count={multiprocessing.cpu_count()}")


    for _ in range(num_runs):
        start_time_load = time.time()
        processed_df = preprocess_parallel(file_path, 40000)
        end_time_load = time.time()
        execution_times.append(end_time_load - start_time_load)

    # Calculate the average execution time
    average_time = sum(execution_times) / num_runs
    print(f"Parallel preprocessing average execution time over {num_runs} runs: {average_time:.4f} seconds")

