import pandas as pd
from app.rules.base_rule import AlertRule
from app.config.settings import ALERT_FATAL_PER_HOUR


class FatalPerHourBundleRule(AlertRule):
    name = "fatal_per_hour_bundle"

    def check(self, df):
        fatal = df[df["severity"] == "fatal"].copy()

        fatal['date'] = pd.to_datetime(fatal['date'], errors='coerce')

        fatal = fatal.dropna(subset=['date'])

        if fatal.empty:
            return pd.Series()

        grouped = fatal.groupby(
            ["bundle_id", pd.Grouper(key="date", freq="1h")]
        ).size()

        return grouped[grouped > ALERT_FATAL_PER_HOUR]