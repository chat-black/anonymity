import json
import sqlite3

import psycopg2 as pg

from assistant import get_username


class DB:
    """
    DB class，responsible for all database related work
    """
    db_name = "ARENA.db"
    DDL = "schema.sql"

    @staticmethod
    def init_db():
        """
        initialize the database defined in schema.sql
        """
        conn = sqlite3.connect(DB.db_name)
        with open(DB.DDL, 'r', encoding='utf-8') as fin:
            conn.executescript(fin.read())
        conn.commit()
        conn.close()

    @staticmethod
    def get_new_uid() -> int:
        conn = sqlite3.connect(DB.db_name, isolation_level=None)
        cur = conn.cursor()

        cur.execute("begin IMMEDIATE;")
        cur.execute("insert into USER(uid) select max(uid)+1 from USER;")
        cur.execute("select max(uid) from USER;")
        temp = cur.fetchall()
        cur.execute("end;")

        cur.close()
        conn.close()

        res = temp[0][0]
        return res

    @staticmethod
    def insert_new_query(sha256: str, sql: str, conf: str) -> bool:
        select_sql = "select * from SQL where hash = ?"
        insert_sql = "insert into SQL(hash, sql, conf) values(?,?,?)"
        res = False

        conn = sqlite3.connect(DB.db_name, isolation_level=None)
        cur = conn.cursor()
        cur.execute("begin exclusive;")

        cur.execute(select_sql, (sha256,))
        temp = cur.fetchall()
        if len(temp) > 0:
            res = True
        else:
            res = False

            cur.execute(insert_sql, (sha256, sql, conf))

            cur.execute("select max(sid) from SQL;")
            temp = cur.fetchall()
            if len(temp) > 0 and temp[0][0] is None:
                cur.execute("update SQL set sid = ? where hash = ?", (0, sha256))
            else:
                temp = int(temp[0][0])
                cur.execute("update SQL set sid = ? where hash = ?", (temp + 1, sha256))
        cur.execute("commit;")
        cur.close()
        conn.close()
        return res

    @staticmethod
    def insert_query_result(sid: int, result):
        query = "insert into QUERY(sid, nid, result) values(?, ?, ?);"
        conn = sqlite3.connect(DB.db_name, isolation_level=None)
        cur = conn.cursor()

        cur.execute("begin IMMEDIATE;")
        for key, value in result.items():
            cur.execute(query, (sid, key, value))
        conn.execute("commit;")
        cur.close()
        conn.close()

    @staticmethod
    def update_db_info(name='tpch'):
        username = get_username()
        pg_conn = pg.connect(database=name, user='wang')
        pg_cur = pg_conn.cursor()

        pg_cur.execute("select tablename from pg_tables where schemaname='public';")
        tables = [i[0] for i in pg_cur.fetchall()]

        res = {}

        for t in tables:
            pg_cur.execute("select column_name, data_type from information_schema.columns where table_name = %s;", (t,))
            col = {i[0]: i[1] for i in pg_cur.fetchall()}
            res[t] = col

        pg_cur.close()
        pg_conn.close()

        conn = sqlite3.connect(DB.db_name, isolation_level=None)
        cur = conn.cursor()

        cur.execute("begin IMMEDIATE;")
        cur.execute("insert into DBINFO values(?, ?);", (name, json.dumps(res)))
        cur.execute("commit;")
        cur.close()
        conn.close()

    @staticmethod
    def insert_feedback(data):
        conn = sqlite3.connect(DB.db_name, isolation_level=None)
        cur = conn.cursor()

        cur.execute("begin IMMEDIATE;")
        for info in data:
            cur.execute("delete from FEEDBACK where uid=? and fileName=? and type =?;",
                        (info["uid"], info["file_name"], info["t"]))
            cur.execute("insert into FEEDBACK(uid, fileName, pid, type, reason) values(?,?,?,?,?)",
                        (info["uid"], info["file_name"], info["pid"], info["t"], info["reason"]));
        cur.execute("commit;")
        cur.close()
        conn.close()

    @staticmethod
    def insert_suggest(uid: int, suggest: str):
        conn = sqlite3.connect(DB.db_name, isolation_level=None)
        cur = conn.cursor()

        cur.execute("begin IMMEDIATE;")
        cur.execute("insert into SUGGEST(uid, info) values(?,?)", (uid, suggest))
        cur.execute("commit;")
        cur.close()
        conn.close()

    @staticmethod
    def insert_pid(pid: int, file_name: str):
        conn = sqlite3.connect(DB.db_name, isolation_level=None)
        cur = conn.cursor()
        cur.execute("begin IMMEDIATE;")
        cur.execute("insert into process values(?, ?);", (pid, file_name))
        cur.execute("commit;")
        cur.close()
        conn.close()

    @staticmethod
    def delete_pid(pid: int):
        conn = sqlite3.connect(DB.db_name, isolation_level=None)
        cur = conn.cursor()
        cur.execute("begin IMMEDIATE;")
        cur.execute("delete from process where pid = ?;", (pid,))
        cur.execute("commit;")
        cur.close()
        conn.close()

    def __init__(self):
        self.conn = sqlite3.connect(DB.db_name)

    def get_query_result(self, sha256: str) -> list:
        cur = self.conn.cursor()
        query = "select nid, result from query, sql where sql.hash = ? and sql.sid = query.sid order by nid;"
        cur.execute(query, (sha256,))
        res = cur.fetchall()
        cur.close()
        return res

    def get_query_id(self, sha256: str) -> int:
        cur = self.conn.cursor()
        query = "select sid from SQL where hash = ?;"
        cur.execute(query, (sha256,))
        res = cur.fetchall()
        cur.close()
        if len(res) > 0:
            return res[0][0]
        else:
            return None

    def get_db_info(self, name: str) -> str:
        cur = self.conn.cursor()
        cur.execute("select info from DBINFO where name=?;", (name,))
        res = cur.fetchall()
        cur.close()
        if len(res) > 0:
            return res[0][0]
        else:
            return ""

    def exist_pid(self, pid: int) -> bool:
        cur = self.conn.cursor()
        cur.execute("select * from process where pid = ?;", (pid,))
        temp = cur.fetchall()
        cur.close()
        return len(temp) > 0

    def get_file_name(self, pid: int):
        cur = self.conn.cursor()
        cur.execute("select fileName from process where pid = ?;", (pid,))
        temp = cur.fetchall()
        cur.close()
        if len(temp) > 0 and len(temp[0]) > 0:
            return temp[0][0]
        else:
            return ""

    def close(self):
        self.conn.close()

    def test(self):
        conn = sqlite3.connect(DB.db_name)
        cur = conn.cursor()
        cur.execute("insert into USER(uid) select max(uid)+1 from USER;")
        cur.execute("insert into USER(uid) select max(uid)+1 from USER;")
        cur.execute("insert into USER(uid) select max(uid)+1 from USER;")
        cur.execute("insert into USER(uid) select max(uid)+1 from USER;")
        cur.execute("insert into USER(uid) select max(uid)+1 from USER;")
        cur.close()
        conn.commit()
        conn.close()
