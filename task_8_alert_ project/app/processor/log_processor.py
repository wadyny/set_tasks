from app.alerting.alert_sender import send_alert


class LogProcessor:
    def __init__(self, reader, rules):
        self.reader = reader
        self.rules = rules

    def process(self):
        print("Starting log processing...")

        total_rows = 0
        data_generator = self.reader.read()

        if data_generator is None:
            return

        for i, df_chunk in enumerate(data_generator):
            if df_chunk.empty:
                continue

            chunk_len = len(df_chunk)
            total_rows += chunk_len

            print(f"--> Processing chunk #{i + 1}: {chunk_len} rows. Total read: {total_rows}")

            for rule in self.rules:
                try:
                    alerts = rule.check(df_chunk)
                    if not alerts.empty:
                        send_alert(rule.name, alerts)
                except Exception as e:
                    print(f"Error processing rule '{rule.name}' on chunk {i}: {e}")

        print(f"Processing finished. Total records processed: {total_rows}")