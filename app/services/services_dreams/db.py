from ...core import settings


USER = settings.mysql_username
PASSWORD = settings.mysql_password
HOSTNAME = settings.mysql_host
DBNAME = settings.mysql_database
# conn = f"mysql+pymysql://{USER}:{PASSWORD}@{HOSTNAME}/{DBNAME}
conn = f"mysql://{USER}:{PASSWORD}@{HOSTNAME}/{DBNAME}"
