import pandas as pd
from app.rules.fatal_per_minute import FatalPerMinuteRule


def test_fatal_per_minute():
    data = {
        "severity": ["fatal"] * 11,
        "date": pd.to_datetime(["2026-01-06 10:00:00"] * 11)
    }

    df = pd.DataFrame(data)
    rule = FatalPerMinuteRule()

    alerts = rule.check(df)


    if not alerts.empty:
        print("Тест пройден")
        print("-" * 20)
        print(alerts)
        print("-" * 20)
    else:
        print("Тест не прошёл")


if __name__ == "__main__":
    try:
        test_fatal_per_minute()
    except Exception as e:
        print(f"Ошибка при запуске: {e}")