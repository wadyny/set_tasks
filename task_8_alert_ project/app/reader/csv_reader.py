import pandas as pd
import os
from app.config.settings import DATA_PATH


class CSVReader:
    def __init__(self):
        self.column_names = [
            'error_code', 'error_message', 'severity', 'log_location', 'mode',
            'model', 'graphics', 'session_id', 'sdkv', 'test_mode', 'flow_id',
            'flow_type', 'sdk_date', 'publisher_id', 'game_id', 'bundle_id',
            'appv', 'language', 'os', 'adv_id', 'gdpr', 'ccpa',
            'country_code', 'date'
        ]

    def read(self, chunksize=100_000):
        print(f"Reading data from {DATA_PATH} in chunks of {chunksize}...")

        if not os.path.exists(DATA_PATH):
            print(f"Error: File {DATA_PATH} not found.")
            return

        try:
            with pd.read_csv(
                    DATA_PATH,
                    names=self.column_names,
                    header=0,
                    chunksize=chunksize
            ) as reader:

                for i, chunk in enumerate(reader):
                    processed_chunk = self._process_chunk(chunk)
                    yield processed_chunk

        except Exception as e:
            print(f"Error reading CSV: {e}")

    def _process_chunk(self, df):
        if df.empty:
            return df

        try:
            first_date = str(df['date'].iloc[0])
            unit = 's' if '.' in first_date else 'ns'

            df['date'] = pd.to_datetime(df['date'], unit=unit, errors='coerce')
            df = df.sort_values('date').reset_index(drop=True)

            return df
        except Exception as e:
            print(f"Warning: Error processing chunk dates: {e}")
            return df