import os
from config import Config
from processor import GridProcessor


def grid_analysis():
    config = Config()

    batch_size = config.batch_size
    if batch_size:
        config.batch_size = int(batch_size)

    processor = GridProcessor(config)

    print("Starting electrical grid analysis...")
    processor.run()
    print("Analysis complete!")


if __name__ == "__main__":
    grid_analysis()
