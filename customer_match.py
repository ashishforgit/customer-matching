import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrameWriter
from pyspark.sql.types import LongType, StructField, StructType,BooleanType
from pyspark.sql.functions import broadcast
import os,csv,time,sys,datetime
import jellyfish
import pandas as pd
#from data_cleaner import check_str_isnumeric,clean_email_string,clean_string

spark = SparkSession.builder.appName("CustomerMatchProcessv0.1").enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()
spark.conf.set("spark.driver.memory",'32g')
spark.conf.set("spark.executor.memory",'40g')
spark.conf.set("spark.executor.cores", 15)
spark.conf.set("spark.shuffle.service.enabled", "true")
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.io.compression.codec", "snappy")
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.consolidateFiles","true")
spark.conf.set("spark.driver.maxResultSize", "5g")
#max_id = spark.sql(""" select nvl(max(customer_key),0) as max_key from dl_business.customer """)
#sc = SparkContext('yarn', 'accumulatoruse')
#customer_seq_acc = sc.accumulator(max_id)
sc = SparkContext.getOrCreate()
sc.addPyFile("./script/customer_match/data_cleaner.py")
import data_cleaner as dc

def dfZipWithIndex (df, offset=1, colName="Customer_ID"):
	new_schema = StructType([StructField(colName,LongType(),True)] + df.schema.fields)
	zipped_rdd = df.rdd.zipWithIndex()
	new_rdd = zipped_rdd.map(lambda (row,Customer_ID): ([Customer_ID+offset] + list(row)))
	return spark.createDataFrame(new_rdd, new_schema)

# def dataframe_table_write(df,tgt_db_name,tgt_table):
	# df_writer = DataFrameWriter(df)
	# df_writer.saveAsTable(tgt_db_name + '.' + tgt_table,format='parquet', mode='overwrite')


# def fuzzy_match_rate_idx(df,col_name_list):
	# import jellyfish
	# import pandas as pd
	# df_mri = pd.DataFrame(df)
	# for col_name in col_name_list:
		# new_col_name = col_name+'_MRI'
		# df_mri[new_col_name] = df[col_name].map(lambda x:jellyfish.match_rating_codex(x))
	
	# return spark.createDataFrame(df_mri)

# def fuzzy_match_rate_codex(s):
	# import jellyfish
	# #import pandas as pd
	# #df_mri = pd.DataFrame(df)
	# return jellyfish.match_rating_codex(s)
	

# def clean_email_string(s):
	# charlist = [",","*","'","\""]
	# for c in charlist:
		# s = s.replace(c,"")
	# return s

# def clean_string(s):
	# charlist = [".",",","*","'","\"","/","!"]
	# for c in charlist:
		# if c == ",":
			# s = s.replace(c," ").strip()
		# else:
			# s = s.replace(c,"").strip()
	# return s

# def check_str_isnumeric(s):
	# charlist = [".",",","*","\"","-","_","!"]
	# for c in charlist:
		# s = s.replace(c,"")
		
	# return s.isdigit()

# def concat_cols(*cols):
    # return F.concat(*[F.coalesce(c, F.lit(";")) for c in cols])

str_isnumeric_udf = F.udf(dc.check_str_isnumeric,BooleanType())
email_clean_udf = F.udf(dc.clean_email_string)
clean_string_udf = F.udf(dc.clean_string)
name_mrc_udf = F.udf(dc.fuzzy_match_rate_codex)

def location_match():
	locDf = spark.sql("""
			select dl_file_timestamp,dl_file_prefix,dl_filename,dl_line_no,user_id,
			concat_ws(' ',nvl(addr_line1,''),nvl(addr_line2,''),nvl(addr_line3,''),nvl(addr_line4,''),nvl(addr_line5,''),nvl(addr_line6,'')) as orig_addr_lines,
			UPPER(TRIM(concat_ws(' ',nvl(addr_line1,''),nvl(addr_line2,''),nvl(addr_line3,''),nvl(addr_line4,''),nvl(addr_line5,''),nvl(addr_line6,' ')))) as std_addr_lines16,
			addr_locality_code as orig_post_code,UPPER(TRIM(regexp_replace(addr_locality_code,'( ){1,}',' '))) std_post_code
			from dl_business.location where trim(addr_locality_code) != ''
			""")
	locDf = locDf.dropDuplicates(['dl_file_prefix','dl_file_timestamp','dl_line_no','std_addr_lines16','std_post_code'])
	locDf = locDf.where( (F.col("std_post_code").isNotNull()) & (F.col("std_post_code") != '-') & (F.col("std_post_code") != '.') & (F.col("std_post_code") != 'NULL') & (F.col("std_post_code") != ''))
	locDf = locDf.withColumn("std_addr_lines",clean_string_udf(F.col("std_addr_lines16")))
	locDf.createOrReplaceTempView("location_clean_data")
	locGrpDf = spark.sql("select std_post_code,std_addr_lines,count(*) from location_clean_data group by std_post_code,std_addr_lines")
	locMatchDF = locDf.join(locGrpDf, (locDf.std_post_code == locGrpDf.std_post_code) & (locDf.std_addr_lines == locGrpDf.std_addr_lines)).select(locDf['dl_file_timestamp'],locDf['dl_file_prefix'],locDf['dl_filename'],locDf['dl_line_no'],locDf['user_id'],locDf['std_addr_lines'],locDf['orig_addr_lines'],locDf['orig_post_code'],locDf['std_post_code'])
	#locMatchDF.createOrReplaceTempView("location_match_data")
	print("Location match step is done")
	return locMatchDF

def customer_name_mri_match():
	actDf = spark.sql("""
			select dl_file_timestamp,dl_file_prefix,dl_filename,dl_line_no,user_id,title,first_name,last_name,middle_names
			from dl_business.account where trim(title) != '' and trim(first_name) != '' and trim(last_name) != ''
			""")
	actDf = actDf.dropDuplicates(['dl_file_prefix','dl_file_timestamp','dl_line_no','title','first_name','last_name','middle_names'])
	actDf = actDf.withColumn("UPR_TITLE",clean_string_udf(F.trim(F.upper(F.col("title"))))).withColumn("UPR_LNAME",clean_string_udf(F.trim(F.upper(F.col("last_name"))))).withColumn("UPR_FNAME",clean_string_udf(F.trim(F.upper(F.col("first_name"))))).withColumn("UPR_MNAME",clean_string_udf(F.trim(F.upper(F.col("middle_names")))))
	actDf = actDf.where( (F.col("UPR_TITLE").isNotNull()) & (F.col("UPR_LNAME").isNotNull()) & (F.col("UPR_FNAME").isNotNull()))
	actDf.createOrReplaceTempView("account_clean_data")
	actlocDF = spark.sql("""
			select a.*,concat_ws('',a.UPR_TITLE,a.UPR_FNAME,a.UPR_LNAME,a.UPR_MNAME) as NAME_STR,b.orig_addr_lines,b.std_addr_lines,b.orig_post_code,b.std_post_code 
			from account_clean_data a,location_match_data b 
			where a.dl_file_timestamp = b.dl_file_timestamp and a.dl_file_prefix = b.dl_file_prefix and a.dl_line_no = b.dl_line_no
			""")
	#actlocDFMRI = fuzzy_match_rate_idx(actlocDF.toPandas(),['NAME_STR'])
	actlocDFMRI = actlocDF.withColumn("NAME_STR_MRI",name_mrc_udf(F.col("NAME_STR")))
	print("Names Match rating codex generation step is done")
	actlocDFMRI.createOrReplaceTempView("account_location_data")
	actlocGrpDF = spark.sql("""
			select NAME_STR_MRI,count(*) 
			from account_location_data 
			group by NAME_STR_MRI
			""")
	#actlocMatchDF = actlocDF.join(actlocGrpDF,(actlocDF.UPR_TITLE == actlocGrpDF.UPR_TITLE) & (actlocDF.UPR_LNAME == actlocGrpDF.UPR_LNAME) & (actlocDF.UPR_FNAME == actlocGrpDF.UPR_FNAME) & (actlocDF.UPR_MNAME == actlocGrpDF.UPR_MNAME)).select(actlocDF['UPR_TITLE'],actlocDF['UPR_FNAME'],actlocDF['UPR_LNAME'],actlocDF['UPR_MNAME'],actlocDF['user_id'],actlocDF['title'],actlocDF['first_name'],actlocDF['last_name'],actlocDF['middle_names'],actlocDF['orig_addr_lines'],actlocDF['std_addr_lines'],actlocDF['orig_post_code'],actlocDF['std_post_code'],actlocDF['dl_file_timestamp'],actlocDF['dl_file_prefix'],actlocDF['dl_filename'],actlocDF['dl_line_no'])
	actlocMatchDF = actlocDFMRI.join(actlocGrpDF,(actlocDFMRI.NAME_STR_MRI == actlocGrpDF.NAME_STR_MRI)).select(actlocDFMRI['UPR_TITLE'],actlocDFMRI['UPR_FNAME'],actlocDFMRI['UPR_LNAME'],actlocDFMRI['UPR_MNAME'],actlocDFMRI['user_id'],actlocDFMRI['title'],actlocDFMRI['first_name'],actlocDFMRI['last_name'],actlocDFMRI['middle_names'],actlocDFMRI['NAME_STR_MRI'],actlocDFMRI['orig_addr_lines'],actlocDFMRI['std_addr_lines'],actlocDFMRI['orig_post_code'],actlocDFMRI['std_post_code'],actlocDFMRI['dl_file_timestamp'],actlocDFMRI['dl_file_prefix'],actlocDFMRI['dl_filename'],actlocDFMRI['dl_line_no'])
	finalExactMatDF = actlocMatchDF.select('UPR_TITLE','UPR_LNAME','UPR_FNAME','UPR_MNAME','std_addr_lines','std_post_code','user_id','title','last_name','first_name','middle_names','NAME_STR_MRI','orig_addr_lines','orig_post_code','dl_filename','dl_file_timestamp','dl_file_prefix','dl_line_no')
	finalExactMatDF.persist()
	finalExactMatDF.createOrReplaceTempView("identity_stage_data")
	print("Names string match step based on match rating codex is done")

def customer_name_exact_match(locMatchDF):
	locMatchDF.createOrReplaceTempView("location_match_data")
	actDf = spark.sql("""
						select dl_file_timestamp,dl_file_prefix,dl_filename,dl_line_no,user_id,title,first_name,last_name,middle_names
						from dl_business.account where trim(title) != '' and trim(first_name) != '' and trim(last_name) != ''
						""")
	actDf = actDf.dropDuplicates(['dl_file_prefix','dl_file_timestamp','dl_line_no','title','first_name','last_name','middle_names'])
	actDf = actDf.withColumn("UPR_TITLE",clean_string_udf(F.trim(F.upper(F.col("title"))))).withColumn("UPR_LNAME",clean_string_udf(F.trim(F.upper(F.col("last_name"))))).withColumn("UPR_FNAME",clean_string_udf(F.trim(F.upper(F.col("first_name"))))).withColumn("UPR_MNAME",clean_string_udf(F.trim(F.upper(F.col("middle_names")))))
	actDf = actDf.where( (F.col("UPR_TITLE").isNotNull()) & (F.col("UPR_LNAME").isNotNull()) & (F.col("UPR_FNAME").isNotNull()))
	actDf.createOrReplaceTempView("account_clean_data")
	actlocDF = spark.sql("""
					select a.*,b.orig_addr_lines,b.std_addr_lines,b.orig_post_code,b.std_post_code 
					from account_clean_data a,location_match_data b 
					where a.dl_file_timestamp = b.dl_file_timestamp and a.dl_file_prefix = b.dl_file_prefix and a.dl_line_no = b.dl_line_no
					""")
	actlocDF.createOrReplaceTempView("account_location_data")
	actlocGrpDF = spark.sql("""
					select UPR_TITLE,UPR_LNAME,UPR_FNAME,UPR_MNAME,count(*) 
					from account_location_data 
					group by UPR_TITLE,UPR_LNAME,UPR_FNAME,UPR_MNAME
					""")
	actlocMatchDF = actlocDF.join(actlocGrpDF,(actlocDF.UPR_TITLE == actlocGrpDF.UPR_TITLE) & (actlocDF.UPR_LNAME == actlocGrpDF.UPR_LNAME) & (actlocDF.UPR_FNAME == actlocGrpDF.UPR_FNAME) & (actlocDF.UPR_MNAME == actlocGrpDF.UPR_MNAME)).select(actlocDF['UPR_TITLE'],actlocDF['UPR_FNAME'],actlocDF['UPR_LNAME'],actlocDF['UPR_MNAME'],actlocDF['user_id'],actlocDF['title'],actlocDF['first_name'],actlocDF['last_name'],actlocDF['middle_names'],actlocDF['orig_addr_lines'],actlocDF['std_addr_lines'],actlocDF['orig_post_code'],actlocDF['std_post_code'],actlocDF['dl_file_timestamp'],actlocDF['dl_file_prefix'],actlocDF['dl_filename'],actlocDF['dl_line_no'])
	nameMatDF = actlocMatchDF.select('UPR_TITLE','UPR_LNAME','UPR_FNAME','UPR_MNAME','std_addr_lines','std_post_code','user_id','title','last_name','first_name','middle_names','orig_addr_lines','orig_post_code','dl_filename','dl_file_timestamp','dl_file_prefix','dl_line_no')
	#nameMatDF.persist()
	#nameMatDF.createOrReplaceTempView("identity_stage_data")
	print("Names string match step based on string exact match is done")
	return nameMatDF
	
def customer_email_match():
	ccDF = spark.sql("""
			select dl_filename,dl_line_no, dl_file_prefix,dl_file_timestamp, lower(trim(email_id)) as email_id
			from dl_business.contact_bkp_18062018 where (email_id is not null OR trim(email_id) = '' OR trim(lower(email_id)) = 'null') 
			group by dl_filename,dl_line_no,dl_file_timestamp,dl_file_prefix,email_id
			""")
	ccDF = ccDF.dropDuplicates(['dl_file_prefix','dl_file_timestamp','dl_line_no','email_id'])
	emailexclDF = spark.sql("""
			select lower(trim(email_address)) as email_address from ods_planninginc_data.scv_email_exclusions
			""")
	ccDF = ccDF.join(broadcast(emailexclDF), ccDF.email_id == emailexclDF.email_address, "left_outer").where(emailexclDF.email_address.isNull()).select([F.col(c) for c in ccDF.columns])
	ccDF = ccDF.withColumn("email_str",F.trim(F.lower(F.split(ccDF['email_id'],"@")[0]))).withColumn("email_domain",F.trim(F.lower(F.split(ccDF['email_id'],"@")[1])))
	ccDFFilt = ccDF.filter( F.col("email_domain").isNotNull() | ~(str_isnumeric_udf(F.col("email_str"))) )
	ccDFFilt = ccDFFilt.na.drop(subset=["email_str","email_domain"])
	ccDFFilt = ccDFFilt.withColumn("std_email_str",email_clean_udf(F.col("email_str"))).withColumn("std_email_domain",email_clean_udf(F.col("email_domain"))).select('dl_filename','dl_line_no','dl_file_prefix','dl_file_timestamp','email_id','std_email_str','std_email_domain')
	ccDFFilt.createOrReplaceTempView("contact_clean_data")
	ccDFGrp = spark.sql("""
				select std_email_str,std_email_domain 
				from contact_clean_data 
				group by std_email_str,std_email_domain
				""")
	ccDFGrp.createOrReplaceTempView("contact_uniq_date")
	emlmatchDF = spark.sql("""
				select a.* 
				from contact_clean_data a,contact_uniq_date b 
				where a.std_email_str = b.std_email_str and a.std_email_domain = b.std_email_domain
				""")
	#emlmatchDF.persist()
	#emlmatchDF.createOrReplaceTempView("contact_match_data")
	print("Email Match Step is done")
	return emlmatchDF

def combine_name_email_match(emlmatchDF,nameMatDF):
	emlmatchDF.persist()
	emlmatchDF.createOrReplaceTempView("contact_match_data")
	nameMatDF.persist()
	nameMatDF.createOrReplaceTempView("identity_stage_data")
	loacccDF = spark.sql("""
					select nvl(a.dl_filename,b.dl_filename) as dl_filename,nvl(a.dl_line_no,b.dl_line_no) as dl_line_no,a.title,a.first_name,a.last_name,a.middle_names,a.upr_title,a.upr_fname,a.upr_lname,a.upr_mname,
					a.orig_addr_lines,a.orig_post_code,a.std_addr_lines,a.std_post_code,b.email_id,concat_ws('@',b.std_email_str,b.std_email_domain) as std_email_id, 
					nvl(a.dl_file_prefix,b.dl_file_prefix) as data_source 
					from contact_match_data b full outer join identity_stage_data a 
					ON (a.dl_file_timestamp = b.dl_file_timestamp and a.dl_file_prefix = b.dl_file_prefix and a.dl_line_no = b.dl_line_no)
					""")
	loacccDF = loacccDF.fillna({'upr_title':'NoTitle','upr_fname':'NoFName','upr_lname':'NoLName','upr_mname':'NoMname','std_addr_lines':'NoAddLn','std_post_code':'NoZipCd','std_email_id':'NoEmail'})
	loacccDF = loacccDF.withColumn("uniqStringKey",F.concat(F.col('upr_title'),F.lit(';'),F.col('upr_lname'),F.lit(';'),F.col('upr_fname'),F.lit(';'),F.col('upr_mname'),F.lit(';'),F.col('std_addr_lines'),F.lit(';'),F.col('std_post_code'),F.lit(';'),F.col('std_email_id')))
	loacccDF.createOrReplaceTempView("acc_loc_cont_match_data")
	loacccDF.persist()
	uniqIdDf = spark.sql("""
					select upr_title as std_title,upr_lname as std_lname,upr_fname as std_fname,upr_mname as std_mnames,std_addr_lines,std_post_code,std_email_id 
					from acc_loc_cont_match_data 
					group by upr_title,upr_lname,upr_fname,upr_mname,std_addr_lines,std_post_code,std_email_id
						""")
	uniqIdDf1 = uniqIdDf.fillna({'std_title':'NoTitle','std_fname':'NoFName','std_lname':'NoLName','std_mnames':'NoMname','std_addr_lines':'NoAddLn','std_post_code':'NoZipCd','std_email_id':'NoEmail'})
	uniqIdDf2 = uniqIdDf1.withColumn("uniqStringKey",F.concat(F.col('std_title'),F.lit(';'),F.col('std_lname'),F.lit(';'),F.col('std_fname'),F.lit(';'),F.col('std_mnames'),F.lit(';'),F.col('std_addr_lines'),F.lit(';'),F.col('std_post_code'),F.lit(';'),F.col('std_email_id')))
	uniqIdDf2.cache()
	#uniqIdDf = spark.sql("select upr_title as std_title,upr_lname std_lname,upr_fname std_fname,upr_mname std_mnames,std_addr_lines,std_post_code from identity_stage_data group by upr_title,upr_lname,upr_fname,upr_mname,std_addr_lines,std_post_code")
	seqDF = dfZipWithIndex(uniqIdDf2)
	seqDF.createOrReplaceTempView("sequenced_unique_match_data")
	#seqDF.persist()

	stgIDDF = spark.sql("""
					select a.Customer_ID,b.title as orig_title,b.last_name as orig_lname,b.first_name as orig_fname,b.middle_names as orig_mnames,
						b.orig_addr_lines,b.orig_post_code,b.email_id as orig_email_id,
						b.dl_filename,b.dl_line_no,1 as match_rule_id, 'STRING EXACT MATCH' as match_rule_desc, b.data_source 
					from sequenced_unique_match_data a,acc_loc_cont_match_data b 
					where a.uniqStringKey = b.uniqStringKey
				""")
	stgIDDF2 = stgIDDF.coalesce(100)
	print("Final match data sets have been created")
	return {'match_details_df':stgIDDF2,'unique_match_df':seqDF}

def customer_match_rs_write(final_rs_dict,p_job_id, p_job_run_id):
	"""
	******** Write Final Datasets into DL_STAGE & DL_BUSINESS table  **********
	"""
	matchdtlDF = final_rs_dict['match_details_df']
	uniqmatchDF = final_rs_dict['unique_match_df']
	matchdtlDF.createOrReplaceTempView("full_acc_loc_email_match_data")
	matchdtlDF.persist()
	uniqmatchDF.createOrReplaceTempView("sequenced_unique_match_data")
	uniqmatchDF.persist()
	#df_writer = DataFrameWriter(stgIDDF2)
	#df_writer.partitionBy('dl_year','dl_month','dl_day').saveAsTable('dl_stage.customer_match_details_tmp',format='parquet', mode='overwrite',path='hdfs:///user/datalake_app_user/processing/businesslayer/identity/customer_full')
	#df_writer.saveAsTable('dl_stage.customer_match_details_tmp',format='parquet', mode='overwrite')
	#df_writer = DataFrameWriter(seqDF)
	#df_writer.saveAsTable('dl_stage.customer_tmp', format='parquet', mode='overwrite')
	dc.dataframe_table_write(matchdtlDF,'dl_stage','customer_match_details_tmp')
	dc.dataframe_table_write(uniqmatchDF,'dl_stage','customer_tmp')	
	
	spark.sql("""
			insert overwrite table dl_business.customer_match_details partition(record_date) 
			select customer_id,
			case when orig_title = 'NoTitle' then null else orig_title end as orig_title,
			case when orig_fname = 'NoFName' then null else orig_fname end as orig_fname,
			case when orig_lname = 'NoLName' then null else orig_lname end as orig_lname,
			case when orig_mnames = 'NoMname' then null else orig_mnames end as orig_mnames,
			case when orig_addr_lines = 'NoAddLn' then null else orig_addr_lines end as orig_addr_lines,
			case when orig_post_code = 'NoZipCd' then null else orig_post_code end as orig_post_code,
			case when orig_email_id = 'NoEmail' then null else orig_email_id end as orig_email_id, '' as mobile_no,'' as daytime_phone,'' as evening_phone, 
			data_source,'' as name_str_mri,dl_filename,dl_line_no,match_rule_id, match_rule_desc,current_timestamp() as insert_dttm,current_timestamp() as modified_dttm, """ +  
			p_job_id + """ as job_id, """ + p_job_run_id + """ as job_run_id, substr(current_timestamp(),1,10)  as record_date
			from full_acc_loc_email_match_data
			""")
	spark.sql("""
			insert overwrite table dl_business.customer partition(record_date) 
			select customer_id,
			case when std_title = 'NoTitle' then null else std_title end as std_title,
			case when std_fname = 'NoFName' then null else std_fname end as std_fname,
			case when std_lname = 'NoLName' then null else std_lname end as std_lname,
			case when std_mnames = 'NoMname' then null else std_mnames end as std_mnames,
			case when std_addr_lines = 'NoAddLn' then null else std_addr_lines end as std_addr_lines,
			case when std_post_code = 'NoZipCd' then null else std_post_code end as std_post_code,
			case when std_email_id = 'NoEmail' then null else std_email_id end as std_email_id,
			'' as mobile_no,'' as daytime_phone,'' as evening_phone,'Y' as active_ind,
			current_timestamp() as insert_dttm,current_timestamp() as modified_dttm, """ + 
			p_job_id + """ as job_id, """ + p_job_run_id + """ as job_run_id, substr(current_timestamp(),1,10)  as record_date
			from sequenced_unique_match_data
		""")

	
def customer_match_update(p_job_id, p_job_run_id):
	start_time = time.time()
	print("Customer match process started at..", datetime.datetime.now())
	# """
	# ********String exact match for Addrelines & Postcode from DL_BUSINESS.Location table data************
	# """
	locMatchDF = location_match()
	# """
	# ******** Filter DL_BUSINESS.Account data to include the records for which address& postcode were matched in previous step AND 
			 # Apply exact string match on name strings from DL_BUSINESS.Account table data************
	# """
	nameMatDF  = customer_name_exact_match(locMatchDF)
	# """
	# ********Identify unique emails from DL_BUSINESS.Contact data and exclude any emails from SCV_email_exclusions table************
	# """
	emlmatchDF = customer_email_match()
	final_rs_dict = combine_name_email_match(emlmatchDF,nameMatDF)
	customer_match_rs_write(final_rs_dict,p_job_id, p_job_run_id)
	print("Time taken : %s seconds ----" % (time.time() - start_time))
	print("Customer match process completed at..", datetime.datetime.now())

def main(args):
	"""
	Args:
      args ([str]): command line parameter list
    """
	# setup logging for driver
	#logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
	#_logger = logging.getLogger(__name__)
	#_logger.info("Starting up...")
	#VENV_DIR = args[0]
    # make sure we have the latest version available on HDFS
	#distribute_hdfs_files('hdfs://' + VENV_DIR)
	#import jellyfish
	#import pandas as pd
	v_job_id = args[0]
	v_job_run_id = args[1]
	customer_match_update(v_job_id,v_job_run_id)
	spark.stop()
	

if __name__ == "__main__":
	main(sys.argv[1:])
	
