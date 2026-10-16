"""State-change notifications; no banking payloads or credentials in messages."""
import json
import os
import smtplib
from email.message import EmailMessage
from urllib.request import Request, urlopen


def notify(key, active, message):
    status = "ALERTE" if active else "RESOLU"
    text = f"LedgerFlow [{status}] {key}: {message}"
    print(json.dumps({"alert": key, "active": active, "message": message}), flush=True)
    webhook = os.environ.get("ALERT_WEBHOOK_URL")
    if webhook:
        request = Request(webhook, data=json.dumps({"text": text, "alert": key,
            "active": active, "message": message}).encode(), headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=10) as response:
            if not 200 <= response.status < 300:
                raise RuntimeError("Webhook rejected notification")
    if os.environ.get("SMTP_HOST"):
        mail = EmailMessage()
        mail["Subject"] = f"LedgerFlow [{status}] {key}"
        mail["From"] = os.environ.get("ALERT_EMAIL_FROM", "ledgerflow@example.test")
        mail["To"] = os.environ.get("ALERT_EMAIL_TO", "operator@example.test")
        mail.set_content(text)
        with smtplib.SMTP(os.environ["SMTP_HOST"], int(os.environ.get("SMTP_PORT", "1025")), timeout=10) as client:
            if os.environ.get("SMTP_STARTTLS", "false").lower() == "true":
                client.starttls()
            if os.environ.get("SMTP_USERNAME"):
                client.login(os.environ["SMTP_USERNAME"], os.environ["SMTP_PASSWORD"])
            client.send_message(mail)


def update_alert(connection, key, active, message, sender=notify):
    # Persist transition first. A failed notification remains pending and is
    # retried next cycle; healthy initial state does not emit a recovery email.
    connection.execute("""
        INSERT INTO analytics.alerts(alert_key,active,message,changed_at,notified_at)
        VALUES (%s,%s,%s,clock_timestamp(),CASE WHEN %s THEN NULL ELSE clock_timestamp() END)
        ON CONFLICT(alert_key) DO UPDATE SET active=EXCLUDED.active,message=EXCLUDED.message,
            changed_at=CASE WHEN analytics.alerts.active<>EXCLUDED.active
                THEN clock_timestamp() ELSE analytics.alerts.changed_at END,
            notified_at=CASE WHEN analytics.alerts.active<>EXCLUDED.active
                THEN NULL ELSE analytics.alerts.notified_at END
        """, (key, active, message[:500], active))
    pending = connection.execute("SELECT notified_at FROM analytics.alerts WHERE alert_key=%s", (key,)).fetchone()[0]
    if pending is None:
        sender(key, active, message)
        connection.execute("UPDATE analytics.alerts SET notified_at=clock_timestamp() WHERE alert_key=%s", (key,))
