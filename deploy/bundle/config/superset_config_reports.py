# --- Alerts & Reports: e-mails with dashboard / chart screenshots -------------------
from superset.tasks.types import FixedExecutor

FEATURE_FLAGS = {"ALERT_REPORTS": True}    # merge with your existing FEATURE_FLAGS
SMTP_HOST = "smtp.example.com"          # company relay
SMTP_PORT = 25
SMTP_STARTTLS = True
SMTP_SSL = False
SMTP_USER = ""
SMTP_PASSWORD = ""
SMTP_MAIL_FROM = "superset@example.com"
EMAIL_REPORTS_SUBJECT_PREFIX = "[Superset] "
ALERT_REPORTS_EXECUTORS = [FixedExecutor("reports_bot")]   # user whose view is captured
WEBDRIVER_TYPE = "chrome"
WEBDRIVER_BASEURL = "http://127.0.0.1:8088/"               # how the workers reach Superset
WEBDRIVER_BASEURL_USER_FRIENDLY = "https://superset.example.com/"
WEBDRIVER_OPTION_ARGS = ["--headless=new", "--disable-gpu", "--disable-dev-shm-usage",
                         "--hide-scrollbars", "--no-sandbox"]
WEBDRIVER_WINDOW = {"dashboard": (1600, 3600), "slice": (1600, 1000), "pixel_density": 1}
