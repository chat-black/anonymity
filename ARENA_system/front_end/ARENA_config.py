import sqlite3
import time

_config_file = 'config.db'


def init():
    sql = """
    CREATE TABLE IF NOT EXISTS config (
    hash text primary key,  
    conf text,  
    target text  
    );
    """
    conn = sqlite3.connect(_config_file)
    cur = conn.cursor()
    cur.executescript(sql)
    cur.close()
    conn.close()


def write_config(sha256: str, conf: str, target: str):
    while True:
        if _is_exist(sha256):
            time.sleep(0.02)
            continue

        conn = sqlite3.connect(_config_file, isolation_level=None)
        cur = conn.cursor()
        cur.execute("begin exclusive;")
        cur.execute("select * from config where hash = ?;", (sha256,))
        temp = cur.fetchall()
        if len(temp) > 0 and len(temp[0]) > 0 and temp[0][0] is not None:
            cur.execute("commit;")
            cur.close()
            conn.close()
            time.sleep(0.02)
            continue
        else:
            cur.execute("insert into config(hash, conf, target) values(?, ?, ?);", (sha256, conf, target))
            cur.execute("commit;")
            cur.close()
            conn.close()
            break


def get_config(sha256: str):
    conn = sqlite3.connect(_config_file)
    cur = conn.cursor()
    cur.execute("select conf, target from config where hash = ?;", (sha256,))
    temp = cur.fetchall()
    cur.close()
    conn.close()
    return temp


def remove_config(sha256: str):
    conn = sqlite3.connect(_config_file)
    cur = conn.cursor()
    cur.execute("delete from config where hash=?;", (sha256,))
    conn.commit()
    cur.close()
    conn.close()


def _is_exist(sha256: str) -> bool:
    conn = sqlite3.connect(_config_file)
    cur = conn.cursor()
    cur.execute("select * from config where hash = ?;", (sha256,))
    temp = cur.fetchall()
    cur.close()
    conn.close()
    return len(temp) > 0 and len(temp[0]) > 0 and temp[0][0] is not None
