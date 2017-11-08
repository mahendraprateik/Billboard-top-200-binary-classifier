from __future__ import division
import pandas as pd
import numpy as np
import pymysql.cursors
import pymysql as pms
import sys #for exit()


class db_connect(object):
	def __init__(self, cred_path):
		self.credential_file = open(cred_path, 'r')
		self.line = self.credential_file.readlines()
		self.all_creds = self.line[0].split(';')
		
	def get_host(self):
		"""
		line = 
		print(line)
		all_creds = line[0].split(';')
		host_cred = all_creds[0]
		"""
		return self.all_creds[0]
	
	def get_user(self):
		"""
		line1 = self.credential_file.readlines()
		print(line1)
		all_creds = line[0].split(';')
		user_cred = all_creds[1]
		"""
		return self.all_creds[1]
		
	def get_pass(self):
		"""
		line = self.credential_file.readlines()
		all_creds = line[0].split(';')
		pw_cred = all_creds[2]
		"""
		return self.all_creds[2]
		
	def get_DBname(self):
		"""
		line = self.credential_file.readlines()
		all_creds = line[0].split(';')
		db_cred = all_creds[3]
		"""
		return self.all_creds[3]
		

class data_subset_gen(object):
	"""
	Class created to contain functions that create useful subsets
	of data: unique_album_ID
	"""
	def __init__(self, cred_path):
		self.dbConn_obj = db_connect(cred_path)
		self.connection = pms.connect(host=self.dbConn_obj.get_host(),
								 user= self.dbConn_obj.get_user(),
								 password=self.dbConn_obj.get_pass(),
								 db=self.dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)
	def All_unique_SoloAlbum_ID(self):
		UniqueSoloAlbumID_clean = []
		try:
			with self.connection.cursor() as cursor:
				cursor.execute("SELECT DISTINCT `key` FROM `AllMusicAlbum` WHERE (`artistm` = 0 AND `year` >= 2000)") #isolating all solo artist albums -- ONLY ALBUMS RELEASED AFTER 2000
				unique_SoloAlbum_ids = cursor.fetchall()
				self.unique_SoloAlbum_ids_DF = pd.DataFrame.from_dict(unique_SoloAlbum_ids)
		finally:
			self.connection.close()
		encoded_unique_SoloAlbum_IDs = np.array(self.unique_SoloAlbum_ids_DF)
		encoded_unique_SoloAlbum_IDs = np.ndarray.tolist(encoded_unique_SoloAlbum_IDs)
		decoded_unique_SoloAlbum_IDs = map(str, encoded_unique_SoloAlbum_IDs)
		for i in decoded_unique_SoloAlbum_IDs:
			just_ID = i[3:15]
			UniqueSoloAlbumID_clean.append(just_ID)
		return UniqueSoloAlbumID_clean
		
	

		
class ftr_fcts(object):
	"""
	Contains functions that all take 1 individual album_id and create the 
	respective feature
	"""
	def __init__(self, indiv_album_ID, cred_path):
		self.indiv_album_ID = indiv_album_ID
		
		"""Establish connection to database """
		
		self.dbConn_obj = db_connect(cred_path)
		self.connection = pms.connect(host=self.dbConn_obj.get_host(),
								 user= self.dbConn_obj.get_user(),
								 password=self.dbConn_obj.get_pass(),
								 db=self.dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)
		self.cred_path = cred_path
		try:
			with self.connection.cursor() as cursor:
				"""Query to retreive artist_ID corresponding to album_ID in question"""
				cursor.execute("SELECT `artists` FROM `AllMusicAlbum` WHERE `key`=%s", (self.indiv_album_ID))
				indiv_alb_artist_ID_dict = cursor.fetchall()
				indiv_alb_artist_ID_DF = pd.DataFrame.from_dict(indiv_alb_artist_ID_dict)
				
				self.indiv_alb_artist_ID = indiv_alb_artist_ID_DF.iloc[0]  #Isolates just the artist_id of interest
				
				#Take artist_ID from Dataframe to list
				indiv_alb_artist_ID_list_unparsed = []
				for i in self.indiv_alb_artist_ID:
					indiv_alb_artist_ID_list_unparsed.append(i)
				
				#Take the bit-endoded artist_ID and isolate just artist_ID
				self.indiv_alb_artist_ID_list_parsed = []
				for i in indiv_alb_artist_ID_list_unparsed:
					self.indiv_alb_artist_ID_list_parsed.append(i[0:13].encode('UTF-8'))
				

				"""Query to retreive the release year of the album_id in question"""
				cursor.execute("SELECT `date`,`year` FROM `AllMusicAlbum` WHERE `key`=%s", (self.indiv_album_ID))
				indiv_alb_releaseYear_dict = cursor.fetchall()
				indiv_alb_releaseYear_DF = pd.DataFrame.from_dict(indiv_alb_releaseYear_dict)
				#Create and fill list with the 1st element in the 'year' column 
				temp_list = []
				temp0 = indiv_alb_releaseYear_DF[['year']].iloc[0]
				for i in temp0:
					temp_list.append(i)
				
				#If None is retreived for year then isolate the last 4 chars from date to get release_year
				
				self.indiv_alb_releaseYear_list = []
				if temp_list[0] is None:
					temp = indiv_alb_releaseYear_DF[['date']].loc[0]
					for i in temp:
						self.indiv_alb_releaseYear_list.append(i[-4:])
				#If something other than None exists in first row of year column retreive that value
				else:
					temp1 = indiv_alb_releaseYear_DF[['year']].iloc[0]
					for i in temp1:
						self.indiv_alb_releaseYear_list.append(i)
				#print(self.indiv_alb_releaseYear_list[0])
				
		finally:
			self.connection.close()
	
	def Genre_parser(self):
		"""GIven one Album it returns a list of all genres ('rock - ma00...')"""
		self.connection = pms.connect(host=self.dbConn_obj.get_host(),
								 user= self.dbConn_obj.get_user(),
								 password=self.dbConn_obj.get_pass(),
								 db=self.dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)
		self.Album_i_Genre_list = []
		try:
			with self.connection.cursor() as cursor:
				cursor.execute("SELECT `genre` FROM `AllMusicAlbum` WHERE `key`=%s", (self.indiv_album_ID))
				album_i_genre_dict = cursor.fetchall()
				
				#If the dict retreived by query is empty:
				if not album_i_genre_dict:
					#then number of albums to date by artist_ID is 0
					nada = 0
					return nada
				elif pd.DataFrame.from_dict(album_i_genre_dict).iloc[0,0] is None:
					return None
				else: 
					album_i_genre_DF = pd.DataFrame.from_dict(album_i_genre_dict)
					album_i_genre_split = album_i_genre_DF.iloc[0,0].split(',')
					for i in range(0, len(album_i_genre_split)):
						album_i_genre_split[i] = album_i_genre_split[i].encode('UTF-8')
					return album_i_genre_split
					#print(album_i_genre_DF)
					#sys.exit()
		finally:
			self.connection.close()
			
	def Num_albums(self):
		"""Given one particular album_id this returns the number of albums released by that album's artist up to the release date of the album"""
		#self.dbConn_obj = db_connect(cred_path)
		self.connection = pms.connect(host=self.dbConn_obj.get_host(),
								 user= self.dbConn_obj.get_user(),
								 password=self.dbConn_obj.get_pass(),
								 db=self.dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)
		
		try:
			with self.connection.cursor() as cursor:	
				"""Query to retreive table of all albums released by artist_ID of album_ID before album_ID's release year"""
				cursor.execute("SELECT * FROM `AllMusicAlbum` WHERE (`artists`=%s AND `year`< %s AND `year` != '')", (self.indiv_alb_artist_ID_list_parsed[0], self.indiv_alb_releaseYear_list[0]))
				
				All_albums_ByArtist_toDate_dict = cursor.fetchall()
				#If the dict retreived by query is empty:
				
				if not All_albums_ByArtist_toDate_dict:
					#then number of albums to date by artist_ID is 0
					nada = 0
					return nada
				
				#If the table retreived by query is not empty
				#else: 
					#then number of albums to date by artist_ID is number of rows in table
				All_albums_ByArtist_toDate_DF = pd.DataFrame.from_dict(All_albums_ByArtist_toDate_dict)
				return len(All_albums_ByArtist_toDate_DF.index)
		finally:
			self.connection.close()
	def Num_ranked_albums(self):
		"""Given one particular artist_id this returns the number of unique albums ranked on BB200 for that artist"""
	
		#self.dbConn_obj = db_connect(cred_path)
		self.connection = pms.connect(host=self.dbConn_obj.get_host(),
								 user= self.dbConn_obj.get_user(),
								 password=self.dbConn_obj.get_pass(),
								 db=self.dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)
								 
		try:
			with self.connection.cursor() as cursor:
				"""Query to return all the various Billboard 200 rankings for artist_id corresponding to album_id"""
				cursor.execute("SELECT * FROM `BBRank` WHERE (`allmusic_artist`=%s AND year(`entry`) <%s)", (self.indiv_alb_artist_ID_list_parsed[0], self.indiv_alb_releaseYear_list[0]))
				All_Ranked_albums_Artist_i_Dict = cursor.fetchall()
				#If the table retreived by query is empty:
					#then number of ranked albums to date by artist_ID is 0
				#If the table retreived by query is not empty:
					#then number of ranked albums to date by artist_ID is number unique unique albums 
				if not All_Ranked_albums_Artist_i_Dict:
					nada = 0
					return nada
				else: 
					All_Ranked_albums_Artist_i_DF = pd.DataFrame.from_dict(All_Ranked_albums_Artist_i_Dict)
					Rank_AlbumID_columnWrepeats = All_Ranked_albums_Artist_i_DF[['allmusic_album']]
					Ranked_Album_tot = len(np.unique(Rank_AlbumID_columnWrepeats))
					return Ranked_Album_tot
					
		finally:
			self.connection.close()
	
	def avg_ranked_albums(self):
		"""Given one particular album_id this returns the average of the BB-ranked albums for that artist at that time"""
		#self.dbConn_obj = db_connect(cred_path)
		self.connection = pms.connect(host=self.dbConn_obj.get_host(),
								 user= self.dbConn_obj.get_user(),
								 password=self.dbConn_obj.get_pass(),
								 db=self.dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)
		
		try:
			with self.connection.cursor() as cursor:
				"""Query to return all the various Billboard 200 rankings for artist_id corresponding to album_id"""
				cursor.execute("SELECT * FROM `BBRank` WHERE (`allmusic_artist`=%s AND year(`entry`) <%s)", (self.indiv_alb_artist_ID_list_parsed[0], self.indiv_alb_releaseYear_list[0]))
				All_Ranked_albums_Artist_i_Dict = cursor.fetchall()
				
				# If the artist is a debut artist or not a debut artist and no previous albums ranked then return None
				if ftr_fcts(self.indiv_album_ID, self.cred_path).Num_albums() == 0 or ftr_fcts(self.indiv_album_ID, self.cred_path).Num_ranked_albums() == 0:
				    return None
					
				#If the table retreived by query is empty:
					#then number of ranked albums to date by artist_ID is 0
				#If the table retreived by query is not empty:
					#then number of ranked albums to date by artist_ID is number unique unique albums 
				if not All_Ranked_albums_Artist_i_Dict:
					nada = 0
					return nada
				else: 
					All_Ranked_albums_Artist_i_DF = pd.DataFrame.from_dict(All_Ranked_albums_Artist_i_Dict)
					All_Rankings_column = All_Ranked_albums_Artist_i_DF[['position']]
					Ranking_avg_unrounded = np.mean(All_Rankings_column)
					return round(Ranking_avg_unrounded, 3)
		finally:
			self.connection.close()
	def stdev_ranked_albums(self):
		"""Given one particular album_id this returns the average of the BB-ranked albums for that artist at that time"""
		#self.dbConn_obj = db_connect(cred_path)
		self.connection = pms.connect(host=self.dbConn_obj.get_host(),
								 user= self.dbConn_obj.get_user(),
								 password=self.dbConn_obj.get_pass(),
								 db=self.dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)
		
		try:
			with self.connection.cursor() as cursor:
				"""Query to return all the various Billboard 200 rankings for artist_id corresponding to album_id"""
				cursor.execute("SELECT * FROM `BBRank` WHERE (`allmusic_artist`=%s AND year(`entry`) <%s)", (self.indiv_alb_artist_ID_list_parsed[0], self.indiv_alb_releaseYear_list[0]))
				All_Ranked_albums_Artist_i_Dict = cursor.fetchall()
				
				# If the artist is a debut artist or not a debut artist and no previous albums ranked then return None
				if ftr_fcts(self.indiv_album_ID, self.cred_path).Num_albums() == 0 or ftr_fcts(self.indiv_album_ID, self.cred_path).Num_ranked_albums() == 0:
				    return None
				
				#If the table retreived by query is empty:
					#then number of ranked albums to date by artist_ID is 0
				#If the table retreived by query is not empty:
					#then number of ranked albums to date by artist_ID is number unique unique albums 
				if not All_Ranked_albums_Artist_i_Dict:
					nada = 0
					return nada
				else: 
					All_Ranked_albums_Artist_i_DF = pd.DataFrame.from_dict(All_Ranked_albums_Artist_i_Dict)
					All_Rankings_column = All_Ranked_albums_Artist_i_DF[['position']]
					Ranking_std_unrounded = np.std(All_Rankings_column)
					return round(Ranking_std_unrounded, 3)
		finally:
			self.connection.close()
			
	def artist_tenure_atm(self):
		"""Given one particular album_id this returns the album's artist's tenure (latest album - oldest album before current album release year)"""
		#self.dbConn_obj = db_connect(cred_path)
		self.connection = pms.connect(host=self.dbConn_obj.get_host(),
								 user= self.dbConn_obj.get_user(),
								 password=self.dbConn_obj.get_pass(),
								 db=self.dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)

		
		try:
			with self.connection.cursor() as cursor:
				"""Query to return all the albums for artist_id corresponding to album_id w year less than current album release year"""
				cursor.execute("SELECT * FROM `AllMusicAlbum` WHERE (`artists`=%s)", (self.indiv_alb_artist_ID_list_parsed[0]))
				All_Ranked_albums_Artist_i_Dict = cursor.fetchall()
				#If the table retreived by query is empty:
					#then tenure to date by artist_ID is 0
				#If the table retreived by query is not empty:
					#then tenure to date by artist_ID is ... 
				if not All_Ranked_albums_Artist_i_Dict:
					#print("I am here")
					nada = 0
					return nada
				else: 
					
					All_albums_Artist_i_DF = pd.DataFrame.from_dict(All_Ranked_albums_Artist_i_Dict)
					colInd_year = All_albums_Artist_i_DF.columns.get_loc('year') #Retreive index number of year column 
					All_albums_Artist_i_arr = np.array(All_albums_Artist_i_DF)
					"""Insert code to loop through the year column and replace the last character with "5" if that last char is a '?'"""
					for n in range(len(All_albums_Artist_i_arr)):
						albYr_field = All_albums_Artist_i_arr[n,colInd_year]
						#print(albYr_field[-1:])
						#sys.exit()
						if albYr_field is None:
							continue
						elif albYr_field[-1:] == '?':
							#albYr_enc = albYr_field.encode('UTF-8')
							All_albums_Artist_i_arr[n,colInd_year] = All_albums_Artist_i_arr[n,colInd_year][:-1]+'5'
						else:
							continue
					#print(All_albums_Artist_i_arr)
					"""
					for i in All_albums_Artist_i_arr[:,colInd_year]:
						if i is None:
							continue
						elif i[:-1] == '?':
							i = i[:-1]+'5'
						else:
							continue
					"""
					Albs_Before_ReleaseYr = All_albums_Artist_i_arr[All_albums_Artist_i_arr[:,colInd_year] < self.indiv_alb_releaseYear_list[0]] 
					Albs_Before_ReleaseYr_df = pd.DataFrame(Albs_Before_ReleaseYr)
					Filtered_Yr_col = Albs_Before_ReleaseYr_df.iloc[:,colInd_year] #just the year column removing albums released after current album X
					Filtered_Yr_col_arr_BitEnc = np.array(Filtered_Yr_col)
					Filtered_Yr_col_arr_parsed = []
					#For loop to get rid of bit encoding 
					for i in Filtered_Yr_col_arr_BitEnc:
						#Accounts for cases where year retreived is type None
						if i is None:
							return 0
						else: 
							Filtered_Yr_col_arr_parsed.append(i.encode('UTF-8'))
					Filtered_Yr_col_arr_Parse_NoEmpty = [x for x in Filtered_Yr_col_arr_parsed if x] #Get rid of empty strings: ''
					Filtered_Yr_col_arr_parsed = map(int,Filtered_Yr_col_arr_Parse_NoEmpty) #Coerce string-val years into integer
					#If list is empty (no albums released at this time) --> tenure = 0
					#Else if only one album released at this time --> tenure = None
					#All other cases --> tenure = max(yearColumn) - min(yearColumn)
					if not Filtered_Yr_col_arr_parsed:
						return 0
					elif len(Filtered_Yr_col_arr_parsed) == 1:
						return None
					else: 
						Earlist_Alb_ReleaseYr = min(Filtered_Yr_col_arr_parsed)
						Most_Recent_Alb_ReleaseYr = max(Filtered_Yr_col_arr_parsed)
						return (Most_Recent_Alb_ReleaseYr - Earlist_Alb_ReleaseYr)
		finally:
			self.connection.close()
		
	def by_genre_wAvg_Num_Albums(self, yrs_before):
		"""Given an album_ID it returns the median number of albums released in the past (yrs_before) years across all genres the album falls under"""
		genre_list = ftr_fcts(self.indiv_album_ID, self.cred_path).Genre_parser()
		#if there are no genres associated w album x --> return None 
		if genre_list is None:
			return None
		else: 
			Num_album_byGenre = []
			window_of_yrs = int(self.indiv_alb_releaseYear_list[0].encode('UTF-8')) - yrs_before
			if window_of_yrs < 2000:
				window_of_yrs = 2000
			self.connection = pms.connect(host=self.dbConn_obj.get_host(),
									 user= self.dbConn_obj.get_user(),
									 password=self.dbConn_obj.get_pass(),
									 db=self.dbConn_obj.get_DBname(),
									 charset='utf8mb4',
									 cursorclass=pms.cursors.DictCursor)
			for i in genre_list:
				self.connection = pms.connect(host=self.dbConn_obj.get_host(),
									 user= self.dbConn_obj.get_user(),
									 password=self.dbConn_obj.get_pass(),
									 db=self.dbConn_obj.get_DBname(),
									 charset='utf8mb4',
									 cursorclass=pms.cursors.DictCursor)
				try:
					with self.connection.cursor() as cursor:
						"""Query to retrieve the number of albums released yrs_before years before current album for one genre from genre_list"""
						cursor.execute("SELECT COUNT(*) FROM `AllMusicAlbum` WHERE `genre` LIKE %s AND %s < `year` < %s", ('%'+i+'%', window_of_yrs, self.indiv_alb_releaseYear_list[0].encode('UTF-8')))
							
						Ith_genre_dict = cursor.fetchall()
						
						Ith_genre_DF = pd.DataFrame.from_dict(Ith_genre_dict)
						Num_album_byGenre.append(Ith_genre_DF.iloc[0,0])
						#print(Ith_genre_DF)
				finally:
					self.connection.close()
			
			"""The following code calculates the weighted average of the number of albums for all the genres the album falls in"""
			sum_albums_allGenres = sum(Num_album_byGenre)
			weights_forAvg = [x / sum_albums_allGenres for x in Num_album_byGenre]
			
			return np.average(Num_album_byGenre, weights = weights_forAvg)
		
	
	def by_genre_wAvg_Num_Ranked_Albums(self, yrs_before):
		"""Given an album_ID it returns the median number of bb-ranked albums released in the past (yrs_before) years across all genres the album falls under"""
		genre_list = ftr_fcts(self.indiv_album_ID, self.cred_path).Genre_parser()
		if genre_list is None:
			return None
		else: 
			Num_Ranked_Album_byGenre = []
			window_of_yrs = int(self.indiv_alb_releaseYear_list[0].encode('UTF-8')) - yrs_before
			if window_of_yrs < 2000:
				window_of_yrs = 2000
			self.connection = pms.connect(host=self.dbConn_obj.get_host(),
								 user= self.dbConn_obj.get_user(),
								 password=self.dbConn_obj.get_pass(),
								 db=self.dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)
			for i in genre_list:
				self.connection = pms.connect(host=self.dbConn_obj.get_host(),
									 user= self.dbConn_obj.get_user(),
									 password=self.dbConn_obj.get_pass(),
									 db=self.dbConn_obj.get_DBname(),
									 charset='utf8mb4',
									 cursorclass=pms.cursors.DictCursor)
				try:
					with self.connection.cursor() as cursor:
						"""Query to retrieve the number of albums released yrs_before years before current album for one genre from genre_list"""
						cursor.execute("SELECT COUNT(*) FROM `AllMusicAlbum` WHERE `genre` LIKE %s AND %s < `year` < %s AND `bb_count` > 0", ('%'+i+'%', window_of_yrs, self.indiv_alb_releaseYear_list[0].encode('UTF-8')))
						
						Ith_genre_dict = cursor.fetchall()
					
						Ith_genre_DF = pd.DataFrame.from_dict(Ith_genre_dict)
						Num_Ranked_Album_byGenre.append(Ith_genre_DF.iloc[0,0])
						
				finally:
					self.connection.close()
			"""The following code calculates the weighted average of the number of ranked albums for all the genres the album falls in"""
			sum_ranked_albums_allGenres = sum(Num_Ranked_Album_byGenre)
			weights_forAvg = [x / sum_ranked_albums_allGenres for x in Num_Ranked_Album_byGenre]
			
			return np.average(Num_Ranked_Album_byGenre, weights = weights_forAvg)
			
	
	def by_genre_wAvg_AvgOfRanked_Albums(self, yrs_before):
		"""Given an album_ID it returns the average of bb-ranked albums released in the past (yrs_before) years across all genres the album falls under"""	
		genre_list = ftr_fcts(self.indiv_album_ID, self.cred_path).Genre_parser()
		if genre_list is None:
			return None
		else: 
			#Need two lists: i) avg of bb-ranked albums for all genres ii) number of bb-ranked albums for all genres
			Num_Ranked_Album_byGenre = []
			AvgOfRanked_Ablums_byGenre = []
			window_of_yrs = int(self.indiv_alb_releaseYear_list[0].encode('UTF-8')) - yrs_before
			if window_of_yrs < 2000:
				window_of_yrs = 2000
			"""Fill up Num_Ranked_Album_byGenre list """
			self.connection = pms.connect(host=self.dbConn_obj.get_host(),
								 user= self.dbConn_obj.get_user(),
								 password=self.dbConn_obj.get_pass(),
								 db=self.dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)
			for i in genre_list:
				self.connection = pms.connect(host=self.dbConn_obj.get_host(),
									 user= self.dbConn_obj.get_user(),
									 password=self.dbConn_obj.get_pass(),
									 db=self.dbConn_obj.get_DBname(),
									 charset='utf8mb4',
									 cursorclass=pms.cursors.DictCursor)
				try:
					with self.connection.cursor() as cursor:
						"""Query to retrieve the number of albums released yrs_before years before current album for one genre from genre_list"""
						cursor.execute("SELECT COUNT(*) FROM `AllMusicAlbum` WHERE `genre` LIKE %s AND %s < `year` < %s AND `bb_count` > 0", ('%'+i+'%', window_of_yrs, self.indiv_alb_releaseYear_list[0].encode('UTF-8')))
						
						Ith_genre_dict = cursor.fetchall()
					
						Ith_genre_DF = pd.DataFrame.from_dict(Ith_genre_dict)
						Num_Ranked_Album_byGenre.append(Ith_genre_DF.iloc[0,0])
						#print(Ith_genre_DF)
				finally:
					self.connection.close()
			"""Fill up AvgOfRanked_Ablums_byGenre list """
			
			self.connection = pms.connect(host=self.dbConn_obj.get_host(),
								 user= self.dbConn_obj.get_user(),
								 password=self.dbConn_obj.get_pass(),
								 db=self.dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)
			for i in genre_list:
				self.connection = pms.connect(host=self.dbConn_obj.get_host(),
									 user= self.dbConn_obj.get_user(),
									 password=self.dbConn_obj.get_pass(),
									 db=self.dbConn_obj.get_DBname(),
									 charset='utf8mb4',
									 cursorclass=pms.cursors.DictCursor)
				try:
					with self.connection.cursor() as cursor:
						"""Query to retrieve the number of albums released yrs_before years before current album for one genre from genre_list"""
						cursor.execute("SELECT AVG(`position`) FROM `AllMusicAlbum` INNER JOIN `BBRank` ON `key` = `allmusic_album` WHERE `genre` LIKE %s AND %s < `year` < %s AND `bb_count` > 0", ('%'+i+'%', window_of_yrs, self.indiv_alb_releaseYear_list[0].encode('UTF-8')))
						
						
						
						Ith_genre_dict = cursor.fetchall()
					
						Ith_genre_DF = pd.DataFrame.from_dict(Ith_genre_dict)
						#print(Ith_genre_DF.iloc[0,0])
						#sys.exit()
						AvgOfRanked_Ablums_byGenre.append(float(Ith_genre_DF.iloc[0,0]))
						#print(Ith_genre_DF)
				finally:
					self.connection.close()
			"""The following code calculates the weighted average of the average of the number of ranked albums for all the genres the album falls in"""
			sum_ranked_albums_allGenres = sum(Num_Ranked_Album_byGenre)
			weights_forAvg = [x / sum_ranked_albums_allGenres for x in Num_Ranked_Album_byGenre]
			
			return np.average(AvgOfRanked_Ablums_byGenre, weights = weights_forAvg)
		
	
	def by_genre_wAvg_StddevOfRanked_Albums(self, yrs_before):
		"""Given an album_ID it returns the average of bb-ranked albums released in the past (yrs_before) years across all genres the album falls under"""	
		genre_list = ftr_fcts(self.indiv_album_ID, self.cred_path).Genre_parser()
		if genre_list is None:
			return None
		else:
			#Need two lists: i) avg of bb-ranked albums for all genres ii) number of bb-ranked albums for all genres
			Num_Ranked_Album_byGenre = []
			StddevOfRanked_Ablums_byGenre = []
			window_of_yrs = int(self.indiv_alb_releaseYear_list[0].encode('UTF-8')) - yrs_before
			if window_of_yrs < 2000:
				window_of_yrs = 2000
				
			"""Fill up Num_Ranked_Album_byGenre list """
			self.connection = pms.connect(host=self.dbConn_obj.get_host(),
								 user= self.dbConn_obj.get_user(),
								 password=self.dbConn_obj.get_pass(),
								 db=self.dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)
			for i in genre_list:
				self.connection = pms.connect(host=self.dbConn_obj.get_host(),
									 user= self.dbConn_obj.get_user(),
									 password=self.dbConn_obj.get_pass(),
									 db=self.dbConn_obj.get_DBname(),
									 charset='utf8mb4',
									 cursorclass=pms.cursors.DictCursor)
				try:
					with self.connection.cursor() as cursor:
						"""Query to retrieve the number of albums released yrs_before years before current album for one genre from genre_list"""
						cursor.execute("SELECT COUNT(*) FROM `AllMusicAlbum` WHERE `genre` LIKE %s AND %s < `year` < %s AND `bb_count` > 0", ('%'+i+'%', window_of_yrs, self.indiv_alb_releaseYear_list[0].encode('UTF-8')))
						
						Ith_genre_dict = cursor.fetchall()
						
						Ith_genre_DF = pd.DataFrame.from_dict(Ith_genre_dict)
						Num_Ranked_Album_byGenre.append(Ith_genre_DF.iloc[0,0])
						
				finally:
					self.connection.close()
			"""Fill up AvgOfRanked_Ablums_byGenre list """
			
			self.connection = pms.connect(host=self.dbConn_obj.get_host(),
								 user= self.dbConn_obj.get_user(),
								 password=self.dbConn_obj.get_pass(),
								 db=self.dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)
			for i in genre_list:
				self.connection = pms.connect(host=self.dbConn_obj.get_host(),
									 user= self.dbConn_obj.get_user(),
									 password=self.dbConn_obj.get_pass(),
									 db=self.dbConn_obj.get_DBname(),
									 charset='utf8mb4',
									 cursorclass=pms.cursors.DictCursor)
				try:
					with self.connection.cursor() as cursor:
						"""Query to retrieve the number of albums released yrs_before years before current album for one genre from genre_list"""
						cursor.execute("SELECT Stddev(`position`) FROM `AllMusicAlbum` INNER JOIN `BBRank` ON `key` = `allmusic_album` WHERE `genre` LIKE %s AND %s < `year` < %s AND `bb_count` > 0", ('%'+i+'%', window_of_yrs, self.indiv_alb_releaseYear_list[0].encode('UTF-8')))
						
						
						
						Ith_genre_dict = cursor.fetchall()
					
						Ith_genre_DF = pd.DataFrame.from_dict(Ith_genre_dict)
						#print(Ith_genre_DF.iloc[0,0])
						#sys.exit()
						StddevOfRanked_Ablums_byGenre.append(float(Ith_genre_DF.iloc[0,0]))
						#print(Ith_genre_DF)
				finally:
					self.connection.close()
			"""The following code calculates the weighted average of the standard deviation of the number of ranked albums for all the genres the album falls in"""
			sum_ranked_albums_allGenres = sum(Num_Ranked_Album_byGenre)
			weights_forAvg = [x / sum_ranked_albums_allGenres for x in Num_Ranked_Album_byGenre]
			
			return np.average(StddevOfRanked_Ablums_byGenre, weights = weights_forAvg)
			
	
class ftr_agg(ftr_fcts):
	"""
	Subclass of ftr_fcts that contains fct that aggregates and 
	creates a 1 x #ftrs vector
	"""
	
	def ftr_aggregation(self):
	
		ftrs_for_artist_i = []
		
		#Calculate & append to list: Number of albums feature
		numAlb_for_artist_i = super(ftr_agg, self).Num_albums()	#Calculate
		ftrs_for_artist_i.append(numAlb_for_artist_i)			#Append
		
		#Calculate & append to list: Number of ranked albums feature
		numRankAlb_for_artist_i = super(ftr_agg, self).Num_ranked_albums()
		ftrs_for_artist_i.append(numRankAlb_for_artist_i)
		
		#Calculate & append to list: Avg position of BB-ranked albums feature
		avgBBRankg_for_artist_i = super(ftr_agg, self).avg_ranked_albums()	#Calculate
		ftrs_for_artist_i.append(avgBBRankg_for_artist_i)
		
		#Calculate & append to list: stdev of position of BB-ranked albums feature
		stdBBRankg_for_artist_i = super(ftr_agg, self).stdev_ranked_albums()	#Calculate
		ftrs_for_artist_i.append(stdBBRankg_for_artist_i)
		
		#Calculate & append to list: Tenure of Artist at Release of album being considered
		tenure_for_artist_i = super(ftr_agg, self).artist_tenure_atm()	#Calculate
		ftrs_for_artist_i.append(tenure_for_artist_i)
		
		#111111111111
		#Calculate & append to list: Weighted Avergae Number of albums in that genre in the past 1 year 
		by_genre_wAvg_Num_Albums_1_i = super(ftr_agg, self).by_genre_wAvg_Num_Albums(1)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_Num_Albums_1_i)
		
		#Calculate & append to list: Weighted Average of Number of ranked albums Number of albums in that genre in the past 1 year 
		by_genre_wAvg_Num_Ranked_Albums_1_i = super(ftr_agg, self).by_genre_wAvg_Num_Ranked_Albums(1)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_Num_Ranked_Albums_1_i)
		
		#Calculate & append to list: Weighted Average of average of ranked albums in that genre in the past 1 year
		by_genre_wAvg_AvgOfRanked_Albums_1_i = super(ftr_agg, self).by_genre_wAvg_AvgOfRanked_Albums(1)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_AvgOfRanked_Albums_1_i)
		
		#Calculate & append to list: Weighted Average of the stddev of the albums in that genre in the past 1 year 
		by_genre_wAvg_StddevOfRanked_Albums_1_i = super(ftr_agg, self).by_genre_wAvg_StddevOfRanked_Albums(1)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_StddevOfRanked_Albums_1_i)
		
		
		#3333333333333
		#Calculate & append to list: Weighted Avergae Number of albums in that genre in the past 3 years
		by_genre_wAvg_Num_Albums_3_i = super(ftr_agg, self).by_genre_wAvg_Num_Albums(3)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_Num_Albums_3_i)
		
		#Calculate & append to list: Weighted Average of Number of ranked albums Number of albums in that genre in the past 3 years
		by_genre_wAvg_Num_Ranked_Albums_3_i = super(ftr_agg, self).by_genre_wAvg_Num_Ranked_Albums(3)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_Num_Ranked_Albums_3_i)
		
		#Calculate & append to list:Weighted Average of average of ranked albums in that genre in the past 3 years
		by_genre_wAvg_AvgOfRanked_Albums_3_i = super(ftr_agg, self).by_genre_wAvg_AvgOfRanked_Albums(3)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_AvgOfRanked_Albums_3_i)
		
		#Calculate & append to list: Weighted Average of the stddev of the albums in that genre in the past 3 years 
		by_genre_wAvg_StddevOfRanked_Albums_3_i = super(ftr_agg, self).by_genre_wAvg_StddevOfRanked_Albums(3)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_StddevOfRanked_Albums_3_i)
		
		#555555555555
		#Calculate & append to list: Weighted Avergae Number of albums in that genre in the past 5 years
		by_genre_wAvg_Num_Albums_5_i = super(ftr_agg, self).by_genre_wAvg_Num_Albums(5)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_Num_Albums_5_i)
		
		#Calculate & append to list: Weighted Average of Number of ranked albums Number of albums in that genre in the past 5 years
		by_genre_wAvg_Num_Ranked_Albums_5_i = super(ftr_agg, self).by_genre_wAvg_Num_Ranked_Albums(5)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_Num_Ranked_Albums_5_i)
		
		#Calculate & append to list: Weighted Average of average of ranked albums in that genre the past 5 years
		by_genre_wAvg_AvgOfRanked_Albums_5_i = super(ftr_agg, self).by_genre_wAvg_AvgOfRanked_Albums(5)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_AvgOfRanked_Albums_5_i)
		
		#Calculate & append to list: Weighted Average of the stddev of the albums in that genre in the past 5 years 
		by_genre_wAvg_StddevOfRanked_Albums_5_i = super(ftr_agg, self).by_genre_wAvg_StddevOfRanked_Albums(5)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_StddevOfRanked_Albums_5_i)
		
		#allllllllllllllllll
		#Calculate & append to list: Weighted Avergae Number of albums in that genre after 2000
		by_genre_wAvg_Num_Albums_all_i = super(ftr_agg, self).by_genre_wAvg_Num_Albums(20)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_Num_Albums_all_i)
		
		#Calculate & append to list: Weighted Average of Number of ranked albums Number of albums in that genre after 2000
		by_genre_wAvg_Num_Ranked_Albums_all_i = super(ftr_agg, self).by_genre_wAvg_Num_Ranked_Albums(20)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_Num_Ranked_Albums_all_i)
		
		#Calculate & append to list: Weighted Average of average of ranked albums in that genre in the past after 2000 
		by_genre_wAvg_AvgOfRanked_Albums_all_i = super(ftr_agg, self).by_genre_wAvg_AvgOfRanked_Albums(20)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_AvgOfRanked_Albums_all_i)
		
		#Calculate & append to list: Weighted Average of the stddev of the albums in that genre in the past after 2000
		by_genre_wAvg_StddevOfRanked_Albums_all_i = super(ftr_agg, self).by_genre_wAvg_StddevOfRanked_Albums(20)	#Calculate
		ftrs_for_artist_i.append(by_genre_wAvg_StddevOfRanked_Albums_all_i)
		

		#convert to array
		ftrs_for_artist_i_arr = np.asarray(ftrs_for_artist_i)
		
		return ftrs_for_artist_i_arr
		

def main():			
	
	"""Creation of Unique Album ID column -- looping thru this -> creation of matrix of features"""
	DSG = data_subset_gen('/home/pm669/db_credentials.txt')		#'/home/pm669/db_credentials.txt'
	UniqueSoloAlbumIDs = DSG.All_unique_SoloAlbum_ID()
	"""
	test1 = ftr_fcts(UniqueSoloAlbumIDs[2024], '/home/pm669/db_credentials.txt').by_genre_wAvg_Num_Albums(5)
	print(test1)
	test2 = ftr_fcts(UniqueSoloAlbumIDs[2024], '/home/pm669/db_credentials.txt').by_genre_wAvg_Num_Ranked_Albums(5)
	print(test2)
	test3 = ftr_fcts(UniqueSoloAlbumIDs[2024], '/home/pm669/db_credentials.txt').by_genre_wAvg_AvgOfRanked_Albums(5)
	print(test3)
	test4 = ftr_fcts(UniqueSoloAlbumIDs[2024], '/home/pm669/db_credentials.txt').by_genre_wAvg_StddevOfRanked_Albums(5)
	print(test4)
	"""
	
	#print("This is the number of solo artist albums  released in 21:", len(UniqueSoloAlbumIDs)) # --> 53746 albums 
	#print(UniqueSoloAlbumIDs[67])
	#test = ftr_fcts(UniqueSoloAlbumIDs[67]).artist_tenure_atm()
	#print(test)
	
	"""testObj = db_connect('/home/pm669/db_credentials.txt')
	testDB = testObj.get_DBname()
	print(testDB)
	"""
	"""Does ftr_aggregation() work? 	----> yes 
	test_artist_obj = ftr_agg('mw0001791556', '/home/pm669/db_credentials.txt')
	test_artist_ftrs = test_artist_obj.ftr_aggregation()
	print(test_artist_ftrs)
	"""
	
	#test = ftr_fcts('mw0001791556').artist_tenure_atm()#album_ID for artist (alicia keys) w BB-ranked albums
	#print(test)
	
	#Opening connection to write values in the sql table
	
	connection = pms.connect(host='azure.xminer.org',
								 user= 'Music',
								 password='lNBPcLcBO4Koq0I51Rajjw',
								 db='Music',
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)
						 
	
	"""test matrix for outputting features"""
	all_artist_ftrs = pd.DataFrame(np.zeros((len(UniqueSoloAlbumIDs), 21)))   #21 is hard-coded # of ftrs/album_ID
	
	#Establish Connection to Database
	
								 
	for i in range(0,len(UniqueSoloAlbumIDs[:])):
		test_ftr_obj = ftr_agg(UniqueSoloAlbumIDs[i], '/home/pm669/db_credentials.txt')
		artist_I_ftrs = test_ftr_obj.ftr_aggregation()
		all_artist_ftrs.iloc[i,:] = artist_I_ftrs
		
		dbConn_obj = db_connect('/home/pm669/db_credentials.txt')
		connection = pms.connect(host=dbConn_obj.get_host(),
								 user= dbConn_obj.get_user(),
								 password=dbConn_obj.get_pass(),
								 db=dbConn_obj.get_DBname(),
								 charset='utf8mb4',
								 cursorclass=pms.cursors.DictCursor)

		#------------------------------my change
		rank = None						 
		
		try:
			with connection.cursor() as cursor:  
				#Query to see if an album id has been ranked on billboard or not
				cursor.execute("SELECT `allmusic_album` from `BBRank` where `allmusic_album` = %s",(UniqueSoloAlbumIDs[i]))
				allmusic_in_bbrank_Dict = cursor.fetchall()
				#If the result contains zero rows then it has never been ranked
				if not allmusic_in_bbrank_Dict:
					rank = 0
				else:
					rank = 1	
        #----------------------------my change					
				params = [UniqueSoloAlbumIDs[i], rank, all_artist_ftrs.iloc[i,0], all_artist_ftrs.iloc[i,1], all_artist_ftrs.iloc[i,2], all_artist_ftrs.iloc[i,3], all_artist_ftrs.iloc[i,4], all_artist_ftrs.iloc[i,5], all_artist_ftrs.iloc[i,6], all_artist_ftrs.iloc[i,7], all_artist_ftrs.iloc[i,8], all_artist_ftrs.iloc[i,9], all_artist_ftrs.iloc[i,10], all_artist_ftrs.iloc[i,11], all_artist_ftrs.iloc[i,12], all_artist_ftrs.iloc[i,13], all_artist_ftrs.iloc[i,14], all_artist_ftrs.iloc[i,15], all_artist_ftrs.iloc[i,16], all_artist_ftrs.iloc[i,17], all_artist_ftrs.iloc[i,18], all_artist_ftrs.iloc[i,19], all_artist_ftrs.iloc[i,20]]
				strParams = [str(params) for x in params]
				newStrParams = strParams[0]
				justCharsParamsV1 = newStrParams.replace("[","")
				justCharsParamsV2 = justCharsParamsV1.replace("]","")
				PassedParams = justCharsParamsV2.split(",")
				alb_id_justChar = PassedParams[0].split("'") 
				#print(alb_id_justChar)
				PassedParams1 = PassedParams
				PassedParams1[0] = alb_id_justChar[1]
				#print(PassedParams)
				#sys.exit()
				#print(PassedParams)
				#print(type(newStrParams))
				cursor.execute("INSERT into `BBPred` (`album_id`,`bb_pred`, `Num_albums`, `Num_ranked_albums`, `avg_ranked_albums`, `stdev_ranked_albums`, `artist_tenure_atm`, `by_genre_wAvg_Num_Albums_1`, `by_genre_wAvg_Num_Ranked_Albums_1`, `by_genre_wAvg_AvgOfRanked_Albums_1`, `by_genre_wAvg_StddevOfRanked_Albums_1`, `by_genre_wAvg_Num_Albums_3`, `by_genre_wAvg_Num_Ranked_Albums_3`, `by_genre_wAvg_AvgOfRanked_Albums_3`, `by_genre_wAvg_StddevOfRanked_Albums_3`, `by_genre_wAvg_Num_Albums_5`, `by_genre_wAvg_Num_Ranked_Albums_5`, `by_genre_wAvg_AvgOfRanked_Albums_5`, `by_genre_wAvg_StddevOfRanked_Albums_5`, `by_genre_wAvg_Num_Albums_all`, `by_genre_wAvg_Num_Ranked_Albums_all`, `by_genre_wAvg_AvgOfRanked_Albums_all` , `by_genre_wAvg_StddevOfRanked_Albums_all`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (PassedParams1[0], PassedParams1[1], PassedParams1[2], PassedParams1[3], PassedParams1[4], PassedParams1[5], PassedParams1[6], PassedParams1[7], PassedParams1[8], PassedParams1[9], PassedParams1[10], PassedParams1[11], PassedParams1[12], PassedParams1[13], PassedParams1[14], PassedParams1[15], PassedParams1[16], PassedParams1[17], PassedParams1[18], PassedParams1[19], PassedParams1[20], PassedParams1[21], PassedParams1[22]))
			
			connection.commit()

				
		finally: 
			connection.close()	
	
	#print(all_artist_ftrs.dtypes)
	#print(type(all_artist_ftrs))
	#print(np.unique(all_artist_ftrs.iloc[:,1]))
	#print(all_artist_ftrs.iloc[0:3,:])
	
	
	
	
	
	
	#print(UniqueSoloAlbumIDs[1])
	#Testing whether ftr creation functions work
	#test = ftr_fcts('mw0000000019').Num_albums() #Album_ID w existing value in `year` of allmusic_album tbl
	#test = ftr_fcts('mw0001791556').Num_albums() --> 4
	#test = ftr_fcts('mw0000588149').Num_albums() --> 0
	#test = ftr_fcts('mw0000000019').Num_ranked_albums() #album_id for artist with no BB-ranked albums 
	#test = ftr_fcts('mw0001791556').Num_ranked_albums()#album_ID for artist (alicia keys) w BB-ranked albums
	
	#print(test)
	
	
	
	
if __name__ == '__main__':	
	main()