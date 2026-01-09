import pandas as pd
from app.rules.base_rule import AlertRule

class ErrorRule(AlertRule):
    name = "network_error_ip"

    def check(self, df):
        # Ищем строки, где сообщение содержит фразу про IP адрес
        # case=False делает поиск нечувствительным к регистру
        network_errors = df[
            df["error_message"].str.contains("could not receive ip address", case=False, na=False)
        ].copy()

        if network_errors.empty:
            return pd.Series()

        # Группируем по минутам. Если больше 10 ошибок в минуту — алерт.
        grouped = network_errors.groupby(
            pd.Grouper(key="date", freq="1min")
        ).size()

        return grouped[grouped > 10]