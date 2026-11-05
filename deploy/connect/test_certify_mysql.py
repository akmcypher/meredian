"""Certifies source_mysql.py with the conformance kit against MySQL 8.4 from
deploy/ingest/test-sources.compose.yml (127.0.0.1:33306). Skips when it is not running."""
import os, socket, unittest
from pathlib import Path
from urllib.parse import urlparse
import pymysql
from conformance import RelationalConformance, ROWS
URL=urlparse(os.environ.get('MYSQL_TEST_URL','mysql://root:meridian-test@127.0.0.1:33306'))
def reachable():
    try: socket.create_connection((URL.hostname,URL.port),2).close(); return True
    except OSError: return False
@unittest.skipUnless(reachable(),'MySQL test source not running (deploy/ingest/test-sources.compose.yml)')
class MySQLConformance(RelationalConformance):
    actor=Path(__file__).parent/'source_mysql.py'
    namespace='connect_certify'
    typed_expected={'big':'9007199254740993','small':42,'amount':'12345678901234567890.123456789','ok':True,'note':'名前 ✓','missing':None,'at':'2026-11-02T10:30:00+00:00','local_at':'2026-11-02T10:30:00','day':'2026-11-02','raw':'3q2+7w==','doc':{'a':[1,'x']}}
    @classmethod
    def config(cls): return {'host':URL.hostname,'port':URL.port,'user':URL.username,'password':URL.password,'database':cls.namespace}
    @classmethod
    def bad_config(cls): return {**cls.config(),'password':'wrong-password'}
    @classmethod
    def sql(cls, *statements, database=True):
        conn=pymysql.connect(host=URL.hostname,port=URL.port,user=URL.username,password=URL.password,database=cls.namespace if database else None,charset='utf8mb4',autocommit=True)
        try:
            with conn.cursor() as c:
                c.execute("SET SESSION time_zone='+00:00', SESSION cte_max_recursion_depth=10000")
                for s in statements: c.execute(s)
        finally: conn.close()
    @classmethod
    def create_fixtures(cls):
        seq=lambda n: f'WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n<{n}) '
        cls.sql('DROP DATABASE IF EXISTS connect_certify','CREATE DATABASE connect_certify CHARACTER SET utf8mb4',database=False)
        cls.sql('CREATE TABLE customers(id bigint PRIMARY KEY,updated_at bigint NOT NULL,name text,email text)',
          f"INSERT INTO customers {seq(ROWS)}SELECT n,n,CONCAT('customer-',n),CONCAT('c',n,'@test') FROM seq WHERE n>0",
          'CREATE TABLE pairs(a int,b int,updated_at bigint NOT NULL,v text,PRIMARY KEY(a,b))',
          f"INSERT INTO pairs {seq(299)}SELECT n DIV 10,n MOD 10,1,CONCAT('v',n) FROM seq",
          'CREATE TABLE typed(id int PRIMARY KEY,updated_at bigint NOT NULL,big bigint,small int,amount decimal(40,9),ok boolean,note text,missing text,at timestamp NULL,local_at datetime,day date,raw varbinary(8),doc json)',
          "INSERT INTO typed VALUES(1,1,9007199254740993,42,12345678901234567890.123456789,true,'名前 ✓',NULL,'2026-11-02 10:30:00','2026-11-02 10:30:00','2026-11-02',X'deadbeef','{\"a\":[1,\"x\"]}')")
    @classmethod
    def touch(cls, ids, cursor): cls.sql(f"UPDATE customers SET updated_at={int(cursor)} WHERE id IN ({','.join(str(int(i)) for i in ids)})")
    @classmethod
    def drop_fixtures(cls): cls.sql('DROP DATABASE IF EXISTS connect_certify',database=False)
if __name__=='__main__': unittest.main()
