import multiprocessing
import time
from src.RealTimeSimulator import simulate_traffic
from src.DataCleaner import start_cleaning_data
from src.StatisticsAggregator import start_aggregating_statistics
from src.CongestionDetector import start_detecting_congestion

if __name__ == '__main__':

    processes = [
        multiprocessing.Process(target=simulate_traffic, name="RealTimeSimulator"),
        multiprocessing.Process(target=start_cleaning_data, name="DataCleaner"),
        multiprocessing.Process(target=start_detecting_congestion, name="CongestionDetector"),
        multiprocessing.Process(target=start_aggregating_statistics, name="Monitor"),   
    ]

    for p in processes:
        p.start()
        time.sleep(1) 

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()