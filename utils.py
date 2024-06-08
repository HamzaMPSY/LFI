import contextlib
import os
import shutil
from configparser import ConfigParser

from loguru import logger


def get_db_credentials(path):
    parser = ConfigParser()
    parser.read(path)
    host = parser.get("database", "host")
    port = parser.get("database", "port")
    dbname = parser.get("database", "dbname")
    user = parser.get("database", "user")
    password = parser.get("database", "password")
    parser.clear()
    return host, port, dbname, user, password


def get_folder_name(path):
    dir_name = path.split("/")[-1].split(".")[0]
    dir_full_path = os.path.join("files", dir_name)
    # Check if it exist already, then remove it an recreate it
    if os.path.exists(dir_full_path):
        shutil.rmtree(dir_full_path, ignore_errors=True)
    os.mkdir(dir_full_path)
    logger.info("Directory %s is created!" % (dir_full_path))
    return dir_name, dir_full_path


def split_large_file(file_path, split_size, dir_name, dir_full_path):
    with contextlib.ExitStack() as stack:
        fd_in = stack.enter_context(
            open(file_path, "r", encoding="ISO-8859-1"))
        for i, line in enumerate(fd_in):
            if not i % split_size:
                if i != 0:
                    fd_out.close()
                file_split = os.path.join(
                    dir_full_path, '{}.{}'.format(dir_name, i//split_size))
                fd_out = stack.enter_context(open(file_split, 'w'))
            fd_out.write('{}'.format(line))
        fd_out.close()
        logger.info("%s has been splitted!" % (file_path))
