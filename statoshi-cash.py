import time,requests,threading,logging,traceback,sys
from influxdb import InfluxDBClient
from os.path import basename

######### Init Logging ############
logdir = "/var/log/"
logpath = "%s%s" % (logdir, basename(sys.argv[0]).replace("py", "log"))
logging.basicConfig(filename=logpath, format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
logging.debug('Logging Activated: %s' % logpath)
##################################
rpc_user = "xxx"
rpc_password = "xxx"
influx_host = "localhost"
influx_port = 8086
influx_user = "root"
influx_password = "xxx"
influx_db = "bitcoincash"
##################################

class blockheader(threading.Thread):
        def __init__(self):
                threading.Thread.__init__(self)

        def run(self):
		startheight = self.last_blockheight
		logging.info('Blockparser startheight at block %s' % startheight)
		while 1:
			try:
				for blockheight in range(startheight,999999):
					headers = { 'content-type': 'text/plain;',}
					data = '{"jsonrpc": "1.0", "id":"curltest", "method": "getblock", "params": ["%s"] }'%(blockheight)
					r = requests.post('http://127.0.0.1:8332/', headers=headers, data=data, auth=(rpc_user, rpc_password))
					r.connection.close()
					r = r.json()['result']
					json_body = [{
						"measurement": "cashnode",
						"tags":
							{"blockheight": r['height'], },
						"fields":
							{
							"difficulty": float(r['difficulty']),
							"blockheight": r['height'],
							"bits": r['bits'],
							"size": float(r['size']),
							"hash": r['hash'],
							"tx_count": len(r['tx']),
							"version": r['versionHex']
							},
						"time": r['time']
					}]
					client = InfluxDBClient(influx_host, influx_port, influx_user, influx_password, influx_db)
					client.write_points(json_body,time_precision='s')
					logging.info('Block processed: %s' % r['height'])
					time.sleep(1)
			except:
				time.sleep(10)
				startheight = blockheight
				logging.debug('Block not processed: %s' % blockheight)

class mempool(threading.Thread):
        def __init__(self):
                threading.Thread.__init__(self)

        def run(self):
		logging.info('Mempoolparser started')
		while 1:
			try:
		        	headers = { 'content-type': 'text/plain;',}
			        data = '{"jsonrpc": "1.0", "id":"curltest", "method": "getmempoolinfo" }'
			        r = requests.post('http://127.0.0.1:8332/', headers=headers, data=data, auth=(rpc_user, rpc_password))
			        r.connection.close()
			        r = r.json()['result']
			        json_body = [{
			                "measurement": "getmempoolinfo",
			                "time": int(time.time()),
		        	        "fields":
		                	        {
			                        "size": float(r['size']),
			                        "bytes": r['bytes'],
			                        "usage": float(r['usage']),
		        	                "maxmempool": r['maxmempool'],
						"avgtxsize": r['bytes']/float(r['size'])
		                        	},
			        }]
				client = InfluxDBClient(influx_host, influx_port, influx_user, influx_password, influx_db)
				client.write_points(json_body,time_precision='s')
				time.sleep(10)
			except:
				time.sleep(10)
				logging.error('Mempool process: %s' % traceback.format_exc())

class estimatefee(threading.Thread):
        def __init__(self):
                threading.Thread.__init__(self)

        def run(self):
		logging.info('Estimatedsmartfeeparser started')
		while 1:
			try:
			        headers = { 'content-type': 'text/plain;',}
			        data = '{"jsonrpc": "1.0", "id":"curltest", "method": "estimatesmartfee", "params": [6] }'
			        r = requests.post('http://127.0.0.1:8332/', headers=headers, data=data, auth=(rpc_user, rpc_password))
		        	r.connection.close()
			        r = r.json()['result']
			        json_body = [{
		        	        "measurement": "estimatesmartfee",
		                	"time": int(time.time()),
			                "fields":
			                        {
			                        "feerate": float(r['feerate'])
		        	                },
			        }]
				client = InfluxDBClient(influx_host, influx_port, influx_user, influx_password, influx_db)
				client.write_points(json_body,time_precision='s')
				time.sleep(60)
			except:
				time.sleep(10)
				logging.error('Estimatesmartfee process: %s' % traceback.format_exc())


class gettxoutsetinfo(threading.Thread):
        def __init__(self):
                threading.Thread.__init__(self)

        def run(self):
		logging.info('gettxoutsetinfo parser started')
		while 1:
			try:
			        headers = { 'content-type': 'text/plain;',}
			        data = '{"jsonrpc": "1.0", "id":"curltest", "method": "gettxoutsetinfo", "params": [] }'
			        r = requests.post('http://127.0.0.1:8332/', headers=headers, data=data, auth=(rpc_user, rpc_password))
		        	r.connection.close()
			        r = r.json()['result']
			        json_body = [{
		        	        "measurement": "gettxoutsetinfo",
		                	"time": int(time.time()),
			                "fields":
			                        {
			                        "height": int(r['height']),
			                        "transactions": int(r['transactions']),
			                        "txouts": int(r['txouts']),
			                        "disk_size": int(r['disk_size']),
			                        "total_amount": float(r['height'])
		        	                },
			        }]
				client = InfluxDBClient(influx_host, influx_port, influx_user, influx_password, influx_db)
				client.write_points(json_body,time_precision='s')
				time.sleep(600)
			except:
				time.sleep(10)
				logging.error('gettxoutsetinfo process: %s' % traceback.format_exc())



bh = blockheader()
bh.last_blockheight = 514593
bh.start()
mp = mempool().start()
sf = estimatefee().start()
utxo = gettxoutsetinfo().start()


