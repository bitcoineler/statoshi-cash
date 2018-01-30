import time,requests,threading,logging,traceback,sys
import socket,time
from influxdb import InfluxDBClient
from os.path import basename

######### Init Logging ############
logdir = "/var/log/"
logpath = "%s%s" % (logdir, basename(sys.argv[0]).replace("py", "log"))
logging.basicConfig(filename=logpath, format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
logging.debug('Logging Activated: %s' % logpath)
##################################
CARBON_SERVER = '0.0.0.0'
CARBON_PORT = 2003
rpc_user = "xxx"
rpc_password = "xxx"
influx_host = "localhost"
influx_port = 8086
influx_user = "root"
influx_password = "xxx"
influx_db = "bitcoincash"
last_blockheight = 514600
##################################

class blockheader(threading.Thread):
        def __init__(self):
                threading.Thread.__init__(self)

        def run(self):
		startheight = self.last_blockheight
		logging.info('Blockparser startheight at block %s' % startheight)
		while 1:
			try:
				for blockheight in range(startheight,999999999999):
					headers = { 'content-type': 'text/plain;',}
					data = '{"jsonrpc": "1.0", "id":"curltest", "method": "getblock", "params": ["%s"] }'%(blockheight)
					r = requests.post('http://127.0.0.1:8332/', headers=headers, data=data, auth=(rpc_user, rpc_password))
					r.connection.close()
					r = r.json()['result']

					message = 'bitcoincash.block.difficulty %s %s\n' %(float(r['difficulty']),r['time'])
					message += 'bitcoincash.block.blockheight %s %s\n' %(int(r['height']),r['time'])
					message += 'bitcoincash.block.bits %s %s\n' %(r['bits'],r['time'])
					message += 'bitcoincash.block.size %s %s\n' %(float(r['size']),r['time'])
					message += 'bitcoincash.block.tx_count %s %s\n' %(len(r['tx']),r['time'])
					message += 'bitcoincash.block.version %s %s\n' %(r['version'],r['time'])

					sock = socket.socket()
					sock.connect((CARBON_SERVER, CARBON_PORT))
					sock.sendall(message)
					sock.close()

					logging.info('Block processed: %s' % r['height'])

					#loop transactions
					'''block_value = float(0)
					message = ""
					for tx in r['tx']:
						headers = { 'content-type': 'text/plain;',}
						data = '{"jsonrpc": "1.0", "id":"curltest", "method": "getrawtransaction", "params": ["%s",1] }'%(tx)
						r = requests.post('http://127.0.0.1:8332/', headers=headers, data=data, auth=(rpc_user, rpc_password))
						r.connection.close()
						r = r.json()['result']

						message = 'bitcoincash.block.tx.%s.size %s %s\n' %(r['txid'],float(r['size']),r['blocktime'])
						message += 'bitcoincash.block.tx.%s.version %s %s\n' %(r['txid'],r['version'],r['blocktime'])
						message += 'bitcoincash.block.tx.%s.locktime %s %s\n' %(r['txid'],r['locktime'],r['blocktime'])
						message += 'bitcoincash.block.tx.%s.vin %s %s\n' %(r['txid'],len(r['vin']),r['blocktime'])
						message += 'bitcoincash.block.tx.%s.vout %s %s\n' %(r['txid'],len(r['vout']),r['blocktime'])
						for vout in r['vout']:
							block_value += vout['value'] 

					message += 'bitcoincash.block.total_value %s %s\n' %(block_value,r['blocktime'])
                                        #sock = socket.socket()
                                        #sock.connect((CARBON_SERVER, CARBON_PORT))
                                        #sock.sendall(message)
                                        #sock.close()'''
				time.sleep(0.2)
			except:
				time.sleep(10)
				startheight = blockheight
				logging.debug('Block not processed: %s %s' % (blockheight,traceback.format_exc()))

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

                                message = 'bitcoincash.getmempoolinfo.size %s %s\n' %(float(r['size']),time.time())
                                message += 'bitcoincash.getmempoolinfo.bytes %s %s\n' %(int(r['bytes']),time.time())
                                message += 'bitcoincash.getmempoolinfo.usage %s %s\n' %(float(r['usage']),time.time())
                                if r['size'] != 0:
					message += 'bitcoincash.getmempoolinfo.avgtxsize %s %s\n' %(r['bytes']/float(r['size']),time.time())
				else:
					message += 'bitcoincash.getmempoolinfo.avgtxsize %s %s\n' %(0,time.time())

                                sock = socket.socket()
                                sock.connect((CARBON_SERVER, CARBON_PORT))
                                sock.sendall(message)
                                sock.close()
				time.sleep(5)
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

                                message = 'bitcoincash.estimatesmartfee.feerate %s %s\n' %(float(r['feerate']),time.time())

                                sock = socket.socket()
                                sock.connect((CARBON_SERVER, CARBON_PORT))
                                sock.sendall(message)
                                sock.close()

				time.sleep(400)
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

                                message = 'bitcoincash.gettxoutsetinfo.height %s %s\n' %(int(r['height']),time.time())
                                message += 'bitcoincash.gettxoutsetinfo.transactions %s %s\n' %(int(r['transactions']),time.time())
                                message += 'bitcoincash.gettxoutsetinfo.txouts %s %s\n' %(int(r['txouts']),time.time())
                                message += 'bitcoincash.gettxoutsetinfo.disk_size %s %s\n' %(int(r['disk_size']),time.time())
                                message += 'bitcoincash.gettxoutsetinfo.total_amount %s %s\n' %(float(r['total_amount']),time.time())

                                sock = socket.socket()
                                sock.connect((CARBON_SERVER, CARBON_PORT))
                                sock.sendall(message)
                                sock.close()

				time.sleep(550)
			except:
				time.sleep(10)
				logging.error('gettxoutsetinfo process: %s' % traceback.format_exc())


class getnetworkhashps(threading.Thread):
        def __init__(self):
                threading.Thread.__init__(self)

        def run(self):
		logging.info('getnetworkhashps parser started')
		while 1:
			try:
			        headers = { 'content-type': 'text/plain;',}
			        data = '{"jsonrpc": "1.0", "id":"curltest", "method": "getnetworkhashps", "params": [] }'
			        r = requests.post('http://127.0.0.1:8332/', headers=headers, data=data, auth=(rpc_user, rpc_password))
		        	r.connection.close()
			        r = r.json()['result']

                                message = 'bitcoincash.getnetworkhashps.hashps %s %s\n' %(float(r),time.time())

                                sock = socket.socket()
                                sock.connect((CARBON_SERVER, CARBON_PORT))
                                sock.sendall(message)
                                sock.close()

				time.sleep(60)
			except:
				time.sleep(10)
				logging.error('getnetworkhashps process: %s' % traceback.format_exc())


class getstat(threading.Thread):
        def __init__(self):
                threading.Thread.__init__(self)

        def run(self):
		logging.info('getstats parser started')
		while 1:
			try:
				statlist = [
  "net/recv/msg/Xb", 
  "net/recv/msg/Xt", 
  "net/recv/msg/addr", 
  "net/recv/msg/block", 
  "net/recv/msg/buverack", 
  "net/recv/msg/buversion", 
  "net/recv/msg/filteradd", 
  "net/recv/msg/filterclear", 
  "net/recv/msg/filterload", 
  "net/recv/msg/filtersizext", 
  "net/recv/msg/get_xblocktx", 
  "net/recv/msg/get_xthin", 
  "net/recv/msg/getaddr", 
  "net/recv/msg/getblocks", 
  "net/recv/msg/getdata", 
  "net/recv/msg/getheaders", 
  "net/recv/msg/headers", 
  "net/recv/msg/inv", 
  "net/recv/msg/mempool", 
  "net/recv/msg/merkleblock", 
  "net/recv/msg/notfound", 
  "net/recv/msg/ping", 
  "net/recv/msg/pong", 
  "net/recv/msg/reject", 
  "net/recv/msg/req_xpedited", 
  "net/recv/msg/sendheaders", 
  "net/recv/msg/thinblock", 
  "net/recv/msg/tx", 
  "net/recv/msg/verack", 
  "net/recv/msg/version", 
  "net/recv/msg/xblocktx", 
  "net/recv/msg/xthinblock", 
  "net/recv/total", 
  "net/send/msg/Xb", 
  "net/send/msg/Xt", 
  "net/send/msg/addr", 
  "net/send/msg/block", 
  "net/send/msg/buverack", 
  "net/send/msg/buversion", 
  "net/send/msg/filteradd", 
  "net/send/msg/filterclear", 
  "net/send/msg/filterload", 
  "net/send/msg/filtersizext", 
  "net/send/msg/get_xblocktx", 
  "net/send/msg/get_xthin", 
  "net/send/msg/getaddr", 
  "net/send/msg/getblocks", 
  "net/send/msg/getdata", 
  "net/send/msg/getheaders", 
  "net/send/msg/headers", 
  "net/send/msg/inv", 
  "net/send/msg/mempool", 
  "net/send/msg/merkleblock", 
  "net/send/msg/notfound", 
  "net/send/msg/ping", 
  "net/send/msg/pong", 
  "net/send/msg/reject", 
  "net/send/msg/req_xpedited", 
  "net/send/msg/sendheaders", 
  "net/send/msg/thinblock", 
  "net/send/msg/tx", 
  "net/send/msg/verack", 
  "net/send/msg/version", 
  "net/send/msg/xblocktx", 
  "net/send/msg/xthinblock", 
  "net/send/total"
]
				message = ""
				for stat in statlist:
			        	headers = { 'content-type': 'text/plain;',}
				        data = '{"jsonrpc": "1.0", "id":"curltest", "method": "getstat", "params": ["%s","sec10"] }'%(stat)
				        r = requests.post('http://127.0.0.1:8332/', headers=headers, data=data, auth=(rpc_user, rpc_password))
			        	r.connection.close()
				        r = r.json()['result']
	
        	                        message += 'bitcoincash.getstats.%s %s %s\n' %(stat,int(r[0]['sec10'][0]),time.time())
	
				print message
                                sock = socket.socket()
               	                sock.connect((CARBON_SERVER, CARBON_PORT))
                       	        sock.sendall(message)
                               	sock.close()

				time.sleep(10)
			except:
				time.sleep(10)
				logging.error('getstats process: %s' % traceback.format_exc())



bh = blockheader()
bh.last_blockheight = last_blockheight
bh.start()
mempool().start()
estimatefee().start()
gettxoutsetinfo().start()
getnetworkhashps().start()
getstat().start()
