import pandas as pd 

def load_data(chank_size):
    raw_data = pd.read_csv("./data/DOT_Traffic_Speeds_NBE.csv",sep=";",  encoding="latin-1", chunksize=chank_size, skiprows=range(1, 1_500_000))

    return raw_data
