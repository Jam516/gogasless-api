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
  SUM(ACTUALGASCOST_USD) AS PAYMASTER_VOLUME,
  COUNT(DISTINCT u.SENDER) AS ACTIVE_ACCOUNTS,
  COUNT(u.OP_HASH) AS GASLESS_TXNS,
  ROW_NUMBER() OVER(ORDER BY COUNT(DISTINCT u.SENDER) DESC) AS RN
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS u
  INNER JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l 
  ON u.CALLED_CONTRACT = l.ADDRESS
  AND l.CATEGORY != 'factory'
  LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APP_METADATA m ON m.NAME = l.NAME
  WHERE u.BLOCK_TIME > CURRENT_DATE - INTERVAL '30 days' 
  AND u.PAYMASTER != '0x0000000000000000000000000000000000000000'
  GROUP BY 1,2,3,4
  ORDER BY 6 DESC
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

