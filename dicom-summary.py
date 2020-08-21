import argparse
import multiprocessing
import os
import sys
from pathlib import Path

import pandas as pd
import pydicom
from pydicom.errors import InvalidDicomError

from joblib import Parallel, delayed
from tqdm import tqdm

num_cores = multiprocessing.cpu_count()

def create_arg_parser():
    """"Creates and returns the ArgumentParser object."""
    parser = argparse.ArgumentParser(
        description="Creates a summary of dicom files found"
    )
    parser.add_argument("inputDirectory", help="Path to the input directory.")
    parser.add_argument(
        "--outputDirectory", help="Path to the output that contains the summary."
    )
    return parser


def read(file):
    try:
        dataset = pydicom.dcmread(str(file), stop_before_pixels=False)
        return {
            "PatientID": dataset.PatientID,
            "AccessionNumber": dataset.AccessionNumber,
            "StudyDescription": dataset.StudyDescription,
            "SeriesDescription": dataset.SeriesDescription,
            "InstanceNumber": dataset.InstanceNumber,
            "KVP" : dataset.KVP
        }
    except InvalidDicomError:
        print(f"File {file} is no dicom file, skipping")
        return {}


def summary(inputDirectory):
    files = tqdm(list(Path(input).glob("**/*.*")))
    print(f"Found {len(files)} in {inputDirectory}")
    
    processed = Parallel(n_jobs=num_cores)(delayed(read)(i) for i in files)
    return processed


if __name__ == "__main__":
    arg_parser = create_arg_parser()
    parsed_args = arg_parser.parse_args(sys.argv[1:])
    input = parsed_args.inputDirectory
    output = parsed_args.outputDirectory
    print("inputDirectory", input)
    print("outputDirectory", output)
    result = summary(Path(input))
    df = pd.DataFrame.from_records(result)
    df[
        [
            "PatientID",
            "AccessionNumber",
            "StudyDescription",
            "SeriesDescription",
            "InstanceNumber",
            "KVP",
        ]
    ].to_csv("summary1.csv", index=False)
    g = df.groupby(
        ["PatientID", "AccessionNumber", "StudyDescription", "SeriesDescription"]
    ).agg(["count"]).reset_index()
    g.to_csv("grouped1.csv", index=False)
