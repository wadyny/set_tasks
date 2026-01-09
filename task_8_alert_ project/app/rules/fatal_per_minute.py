import pandas as pd
from app.rules.base_rule import AlertRule
from app.config.settings import ALERT_FATAL_PER_MINUTE

class FatalPerMinuteRule(AlertRule):
    name = "fatal_per_minute"

    def check(self, df):
        fatal = df[df["severity"] == "fatal"].copy()

        fatal['date'] = pd.to_datetime(fatal['date'], errors='coerce')

        fatal = fatal.dropna(subset=['date'])

        if fatal.empty:
            return pd.Series()

        grouped = fatal.groupby(
            pd.Grouper(key="date", freq="1min")
        ).size()

        return grouped[grouped > ALERT_FATAL_PER_MINUTE]