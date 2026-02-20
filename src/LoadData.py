import pandas as pd 

def load_data(chank_size):
    raw_data = pd.read_csv("./data/DOT_Traffic_Speeds_NBE.csv",sep=";",  encoding="latin-1", chunksize=chank_size)

    return raw_data
