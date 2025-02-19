from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_caching import Cache
import snowflake.connector
from snowflake.connector import DictCursor
import os

REDIS_LINK = os.environ['REDIS']
SNOWFLAKE_USER = os.environ['SNOWFLAKE_USER']
SNOWFLAKE_PASS = os.environ['SNOWFLAKE_PASS']
SNOWFLAKE_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_WAREHOUSE = os.environ['SNOWFLAKE_WAREHOUSE']

config = {
  "CACHE_TYPE": "redis",
  "CACHE_DEFAULT_TIMEOUT": 21600,
  "CACHE_REDIS_URL": REDIS_LINK
}

app = Flask(__name__)
app.config.from_mapping(config)
cache = Cache(app)
CORS(app)


def make_cache_key(*args, **kwargs):
  path = request.path
  args = str(hash(frozenset(request.args.items())))
  return (path + args).encode('utf-8')


def execute_sql(sql_string, **kwargs):
  conn = snowflake.connector.connect(user=SNOWFLAKE_USER,
                                     password=SNOWFLAKE_PASS,
                                     account=SNOWFLAKE_ACCOUNT,
                                     warehouse=SNOWFLAKE_WAREHOUSE,
                                     database="BUNDLEBEAR",
                                     schema="DBT_KOFI")

  sql = sql_string.format(**kwargs)
  try:
    res = conn.cursor(DictCursor).execute(sql)
    results = res.fetchall()
  except Exception as e:
    print(f"An error occurred while executing the SQL query: {sql}")
    raise e
  finally:
    conn.close()
  return results



@app.route('/home')
@cache.memoize(make_name=make_cache_key)
def home():
  timeframe = request.args.get('timeframe', 'month')

  leaderboard = execute_sql('''
  SELECT 
      COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
      COALESCE(m.LOGO, 'https://tspekraxapsoevhxjafh.supabase.co/storage/v1/object/public/logos//other.png') AS LOGO, 
      m.WEBSITE,
      m.CATEGORY,
    
      SUM(CASE WHEN u.BLOCK_TIME > CURRENT_DATE - INTERVAL '7 days' THEN ACTUALGASCOST_USD ELSE 0 END) AS PAYMASTER_VOLUME_7D,
      COUNT(DISTINCT CASE WHEN u.BLOCK_TIME > CURRENT_DATE - INTERVAL '7 days' THEN u.SENDER END) AS ACTIVE_ACCOUNTS_7D,
      COUNT(CASE WHEN u.BLOCK_TIME > CURRENT_DATE - INTERVAL '7 days' THEN u.OP_HASH END) AS GASLESS_TXNS_7D,
      ROW_NUMBER() OVER(ORDER BY COUNT(DISTINCT CASE WHEN u.BLOCK_TIME > CURRENT_DATE - INTERVAL '7 days' THEN u.SENDER END) DESC) AS RN_7D,

      SUM(CASE WHEN u.BLOCK_TIME > CURRENT_DATE - INTERVAL '30 days' THEN ACTUALGASCOST_USD ELSE 0 END) AS PAYMASTER_VOLUME_30D,
      COUNT(DISTINCT CASE WHEN u.BLOCK_TIME > CURRENT_DATE - INTERVAL '30 days' THEN u.SENDER END) AS ACTIVE_ACCOUNTS_30D,
      COUNT(CASE WHEN u.BLOCK_TIME > CURRENT_DATE - INTERVAL '30 days' THEN u.OP_HASH END) AS GASLESS_TXNS_30D,
      ROW_NUMBER() OVER(ORDER BY COUNT(DISTINCT CASE WHEN u.BLOCK_TIME > CURRENT_DATE - INTERVAL '30 days' THEN u.SENDER END) DESC) AS RN_30D,

      SUM(CASE WHEN u.BLOCK_TIME > CURRENT_DATE - INTERVAL '90 days' THEN ACTUALGASCOST_USD ELSE 0 END) AS PAYMASTER_VOLUME_90D,
      COUNT(DISTINCT CASE WHEN u.BLOCK_TIME > CURRENT_DATE - INTERVAL '90 days' THEN u.SENDER END) AS ACTIVE_ACCOUNTS_90D,
      COUNT(CASE WHEN u.BLOCK_TIME > CURRENT_DATE - INTERVAL '90 days' THEN u.OP_HASH END) AS GASLESS_TXNS_90D,
      ROW_NUMBER() OVER(ORDER BY COUNT(DISTINCT CASE WHEN u.BLOCK_TIME > CURRENT_DATE - INTERVAL '90 days' THEN u.SENDER END) DESC) AS RN_90D
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS u
  INNER JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l 
      ON u.CALLED_CONTRACT = l.ADDRESS
      AND l.CATEGORY != 'factory'
  LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APP_METADATA m 
      ON m.NAME = l.NAME
  WHERE u.BLOCK_TIME > CURRENT_DATE - INTERVAL '90 days' 
      AND u.BLOCK_TIME < CURRENT_DATE
      AND u.PAYMASTER != '0x0000000000000000000000000000000000000000'
  GROUP BY 1,2,3,4
  ORDER BY ACTIVE_ACCOUNTS_30D DESC
  ''')

  total_paymaster_stats = execute_sql('''
  SELECT 
  COUNT(OP_HASH) AS GASLESS_TXNS,
  SUM(ACTUALGASCOST_USD) AS PAYMASTER_VOLUME
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
  WHERE PAYMASTER != '0x0000000000000000000000000000000000000000' 
  ''')

  response_data = {
    "leaderboard": leaderboard,
    "total_paymaster_stats": total_paymaster_stats
  }

  return jsonify(response_data)


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=81)

