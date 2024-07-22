import requests
import json
import psycopg2
from psycopg2 import sql
import datetime

# 比特币 RPC 配置
RPC_USER = 'your_user_name'
RPC_PASSWORD = 'your_password'
RPC_PORT = 8332  # 比特币 RPC 默认端口

# PostgreSQL 配置
PG_HOST = 'localhost'
PG_PORT = '5432'
PG_DATABASE = 'postgres'
PG_USER = 'postgres'
PG_PASSWORD = 'postgres'

def bitcoin_rpc(method, params=[]):
    url = f'http://127.0.0.1:{RPC_PORT}/'
    headers = {'content-type': 'application/json'}
    data = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": "1"
    }
    response = requests.post(url, headers=headers, data=json.dumps(data), auth=(RPC_USER, RPC_PASSWORD))
    return response.json()

def connect_to_pg():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    return conn

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS blocks (
                block_hash VARCHAR(64) PRIMARY KEY,
                block_height INTEGER,
                previous_block_hash VARCHAR(64),
                merkle_root VARCHAR(64),
                timestamp TIMESTAMP,
                nonce BIGINT,
                difficulty FLOAT,
                confirmations INTEGER
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                txid VARCHAR(64) PRIMARY KEY,
                block_hash VARCHAR(64) REFERENCES blocks(block_hash),
                version INTEGER,
                size INTEGER,
                lock_time BIGINT
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS inputs (
                input_id SERIAL PRIMARY KEY,
                txid VARCHAR(64) REFERENCES transactions(txid),
                input_index INTEGER,
                previous_txid VARCHAR(64),
                previous_output_index INTEGER,
                script_sig TEXT,
                sequence BIGINT
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS outputs (
                output_id SERIAL PRIMARY KEY,
                txid VARCHAR(64) REFERENCES transactions(txid),
                output_index INTEGER,
                value DECIMAL(18, 8),
                script_pub_key TEXT,
                address VARCHAR(100)
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS op_return (
                txid VARCHAR(64) PRIMARY KEY REFERENCES transactions(txid),
                op_return_data TEXT
            )
        """)
    conn.commit()

def insert_block_data(conn, block_info):
        cur.execute(sql.SQL("""
            INSERT INTO blocks (block_hash, block_height, previous_block_hash, merkle_root, timestamp, nonce, difficulty, confirmations)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (block_hash) DO NOTHING
        """), (
            block_info['hash'],
            block_info['height'],
            block_info['previousblockhash'],
            block_info['merkleroot'],
            timestamp_to_datetime(block_info['time']),
            block_info['nonce'],
            block_info['difficulty'],
            block_info['confirmations']
        ))

def insert_tx_data(conn, tx):
        cur.execute(sql.SQL("""
            INSERT INTO transactions (txid, block_hash, version, size, lock_time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (txid) DO NOTHING
        """), (
            tx['txid'],
            block_info['hash'],
            tx['version'],
            tx['size'],
            tx['locktime']
        ))

        for index, vin in enumerate(tx['vin']):
            cur.execute(sql.SQL("""
                INSERT INTO inputs (txid, input_index, previous_txid, previous_output_index, script_sig, sequence)
                VALUES (%s, %s, %s, %s, %s, %s)
            """), (
                tx['txid'],
                index,
                vin.get('txid', None),
                vin.get('vout', None),
                vin.get('scriptSig', {}).get('asm', None),
                vin.get('sequence', None)
            ))

        for index, vout in enumerate(tx['vout']):
            cur.execute(sql.SQL("""
                INSERT INTO outputs (txid, output_index, value, script_pub_key, address)
                VALUES (%s, %s, %s, %s, %s)
            """), (
                tx['txid'],
                index,
                vout['value'],
                vout['scriptPubKey']['asm'],
                vout['scriptPubKey'].get('addresses', [None])[0] if 'addresses' in vout['scriptPubKey'] else None
            ))

            if vout['scriptPubKey']['type'] == 'nulldata':
                cur.execute(sql.SQL("""
                    INSERT INTO op_return (txid, op_return_data)
                    VALUES (%s, %s)
                    ON CONFLICT (txid) DO NOTHING
                """), (
                    tx['txid'],
                    vout['scriptPubKey']['asm']
                ))

def timestamp_to_datetime(timestamp):
    return datetime.datetime.fromtimestamp(timestamp)

if __name__ == "__main__":
    # 连接到 PostgreSQL
    conn = connect_to_pg()

    # 创建表格（如果不存在）
    create_tables(conn)

    # 获取当前最新区块高度
    # block_count = bitcoin_rpc("getblockcount")['result']
    start = 840000
    end = 840143
    with conn.cursor() as cur:
        for i in range(start, end+1):
            print("%d of %d"%(i-start+1,end+1-start))
            # 获取区块
            block_hash = bitcoin_rpc("getblockhash", [i])['result']
            block_info = bitcoin_rpc("getblock", [block_hash])['result']

            # 插入区块数据到 PostgreSQL
            insert_block_data(conn, block_info)

            for tx in block_info['tx']:
                tx_hex = bitcoin_rpc("getrawtransaction", [tx])['result']
                tx_info = bitcoin_rpc("decoderawtransaction", [tx_hex])['result']
                insert_tx_data(conn, tx_info)
    conn.commit()
    # 关闭 PostgreSQL 连接
    conn.close()
