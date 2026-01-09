def send_alert(rule_name, alerts):
    for key, count in alerts.items():
        print(f"[ALERT] {rule_name} → {key} ({count} fatal errors)")
