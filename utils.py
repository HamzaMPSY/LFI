from configparser import ConfigParser


def get_db_credintials(path):
    parser = ConfigParser()
    parser.read(path)
    host = parser.get("database", "host")
    port = parser.get("database", "port")
    dbname = parser.get("database", "dbname")
    user = parser.get("database", "user")
    password = parser.get("database", "password")
    parser.clear()
    return host, port, dbname, user, password
