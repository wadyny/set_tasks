class AlertRule:
    name = "base_rule"

    def check(self, df):
        raise NotImplementedError
