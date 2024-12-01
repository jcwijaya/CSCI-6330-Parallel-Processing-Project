import pandas as pd
import numpy as np
import time
from concurrent.futures import ProcessPoolExecutor
import warnings
warnings.filterwarnings('ignore',category=FutureWarning)


def process_replace(chunk):
    replacements = {
        'USMER': {2: 0},
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


def process_chunk(chunk):
    # Replicate the dataset 10 times
    chunk = pd.concat([chunk] *30, ignore_index=True)

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


def preprocess_concurrent(file_path, chunksize=40000):
    processed_chunks = []

    # Read data in chunks
    with pd.read_csv(file_path, chunksize=chunksize) as reader:
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(process_chunk, chunk) for chunk in reader]

            for future in futures:
                processed_chunks.append(future.result())

    # Combine all processed chunks into a single DataFrame
    return pd.concat(processed_chunks, ignore_index=True)


if __name__ == '__main__':
    file_path = '../covid-data.csv'
    num_runs = 1  # Number of times to run the function
    execution_times = []

    for _ in range(num_runs):
        start_time_load = time.time()
        processed_df = preprocess_concurrent(file_path, chunksize=40000)
        end_time_load = time.time()
        execution_times.append(end_time_load - start_time_load)

    # Calculate the average execution time
    average_time = sum(execution_times) / num_runs
    print(f"Concurrent preprocessing average execution time over {num_runs} runs: {average_time:.4f} seconds")
