from matplotlib import pyplot as plt
import os
import pandas as pd
from datetime import datetime
import numpy as np

dir = "./part4/results/latest/q1"
percentiles = [5, 10, 50, 67, 75, 80, 85, 90, 95, 99, 999, 9999]
output_dir = "./part4/results/latest/q1"

def transform_cpu_measures(cores: int):
    file_path = None
    if cores == 1:
        file_path = f"{dir}/1_core_2interval/1_core_2interval_cpu.txt"
    elif cores == 2:
        file_path = f"{dir}/2_core_3interval/2_core_3interval_cpu.txt"
    data = []
    with open(file_path, "r") as file:
        bins = file.read()
        bins = bins.split("\n\n")
        for bin in bins:
            bin = bin.split("\n")
            print(bin)
            print("")

transform_cpu_measures(2)