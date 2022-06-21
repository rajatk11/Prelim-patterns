
class polygon_data_pull_util :

	"""
	Libraries Required : os, datetime, polygon, pandas, logging
	Usage :
	- Create/initialize object with your api_key and log_file location
	- Call the object's pull_data function with required args listed in the func definition 
	
	Eg :
	loader = polygon_data_pull_util(your_api_key, '/Users/logs/data_pull/')
	symb = ['AAPL', 'MSFT']
	loader.pull_data(symb, [5, 'minute'], '2021-12-01', '2022-05-24', '/Users/Documents/Test/data/')

	"""
	def __init__(self, api_key, log_locn) :
		self.log_locn = log_locn
		self.api_key = api_key
		now = datetime.datetime.now()
		tmst = now.strftime("%m_%d_%Y_%H_%M_%S")
		if not os.path.exists(log_locn) :
			print('creating log folder')
			os.mkdir(log_locn)
		self.log_file = os.path.join(log_locn, 'loader_'+tmst+'.log') 
		logging.basicConfig(filename=self.log_file, filemode='w')


	def date_slicer(self, start_date, end_date) :
		stdate = datetime.datetime.strptime(start_date, '%Y-%m-%d')
		now = datetime.datetime.now()
		if stdate > now :
			logging.error('Start date in the future')
			return
		edate = datetime.datetime.strptime(end_date, '%Y-%m-%d')
		edate  = min(edate, now)
		
		intervals = []
		curr = stdate
		while curr < edate :
			c_end = min(datetime.datetime.now(), curr + datetime.timedelta(days = 20))
			d1 = str(curr.year) + '-' + "{:02d}".format(curr.month) + '-' + "{:02d}".format(curr.day)
			d2 = str(c_end.year) + '-' + "{:02d}".format(c_end.month) + '-' + "{:02d}".format(c_end.day)
			intervals.append([d1,d2])
			curr = c_end + datetime.timedelta(days = 1)
		return intervals



	def pull_data(self, symbols, freq, start_date, end_date, locn) :
		
		"""
		symbols : list of symbols to download
		freq : list - format eg -> [10, 'minute']   > check polygon API docs : https://polygon.io/docs/stocks/get_v2_aggs_ticker__stocksticker__range__multiplier___timespan___from___to
		start_date, end_date : 'yyyy-mm-dd'
		locn : to save the data

		""" 
		
		intervals = self.date_slicer(start_date, end_date)

		for sym in symbols :
			tslist = []
			newlist = []
			if os.path.exists(locn+sym) :
				logging.info('exists :', locn+sym)
			else :
				logging.info('creating :', locn+sym)
				os.mkdir(locn+sym)
			for interval in intervals :
				logging.info('Processing ' + sym + ' : ' +  interval[0])
				with polygon.RESTClient(self.api_key) as client :
					try :
						resp = client.stocks_equities_aggregates(sym, freq[0], freq[1], interval[0], interval[1], adjusted = True, limit = 500000)
						assert resp.ticker == sym
						for rec in resp.results :
							try :
								newlist.append([rec['o'], rec['h'], rec['l'], rec['c'], rec['v'], rec['vw'], rec['n']])
								timest = (datetime.datetime.fromtimestamp(rec['t']/1000))
								tslist.append(timest)
							except :
								logging.error(f'Error for {sym} at {interval[0]}', exc_info=True)
					except :
						logging.error(f'API call FAILED for {sym} at {interval[0]}', exc_info=True)

					time.sleep(12)  # to comply with free API restrictions; comment if subscribed
			logging.info('Download successful for : ', sym)

			cols = ['Open', 'High', 'Low', 'Close', 'Vol', 'vwPrice', 'Numbars']
			newdf = pd.DataFrame(newlist, columns = cols, index = tslist)
			newdf.to_pickle(locn+sym+'/'+sym+str(freq[0])+freq[1]+'.pkl')




