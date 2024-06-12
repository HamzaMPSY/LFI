from DBConnection import DBConnection
from Lfi import Lfi
from utils import get_db_credentials

if __name__ == "__main__":
    host, port, dbname, user, password = get_db_credentials(".config")
    connection = DBConnection(host, port, dbname, user, password)
    lfi = Lfi(
        path="files",
        connection=connection,
        split_size=10000,
        chunk_size=1024,
        schema="mpsy",
    )
    lfi.start()
