from app.reader.csv_reader import CSVReader
from app.processor.log_processor import LogProcessor
from app.rules.fatal_per_minute import FatalPerMinuteRule
from app.rules.fatal_per_hour_bundle import FatalPerHourBundleRule
from app.rules.error_rule import ErrorRule

def main():

    reader = CSVReader()
    rules = [
        FatalPerMinuteRule(),
        FatalPerHourBundleRule(),
        ErrorRule()
    ]

    processor = LogProcessor(reader, rules)

    processor.process()

    print("Processing finished")


if __name__ == "__main__":
    main()