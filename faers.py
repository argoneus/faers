# Dr. Nicholas Davis, 9-18-15
# Process FDA Adverse Event Reporting System (FAERS) data using Spark
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sqlContext = SQLContext(sc)

# HDFS location of FAERS data files
hdfs_path = "hdfs://172.17.0.9:9000/user/root/faers"

# Each 'partition' of a category has a distinct header, thus must be read and 
# grouped individually initially
categories = ['DEMO1', 'DEMO2', 'DEMO3', 'DEMO4', 'DRUG1', 'DRUG2', 'DRUG3', 
			'INDI1', 'INDI2', 'OUTC1', 'OUTC2', 'REAC1', 'REAC2', 'REAC3', 
			'RPSR1', 'RPSR2', 'THER1', 'THER2']

groupings = ['{DEMO04Q[1-4]*,DEMO05Q[1-2]*}', '{DEMO05Q[3-4]*,DEMO0[6-9]*,DEMO1[0-1]*,DEMO12Q[1-3]*}', 
			'{DEMO12Q4*,DEMO13*,DEMO14Q[1-2]*}', '{DEMO14Q[3-4]*,DEMO15*}', '{DRUG0[4-9]*,DRUG1[0-1]*,DRUG12Q[1-3]*}',
			'{DRUG12Q4*,DRUG13*,DRUG14Q[1-2]*}', '{DRUG14Q[3-4]*,DRUG15*}', '{INDI0[4-9]*,INDI1[0-1]*,INDI12Q[1-3]*}',
			'{INDI12Q4*,INDI1[3-5]*}', '{OUTC0[4-9]*,OUTC1[0-1]*,OUTC12Q[1-3]*}', '{OUTC12Q4*,OUTC1[3-5]*}',
			'{REAC0[4-9]*,REAC1[0-1]*,REAC12Q[1-3]*}', '{REAC12Q4*,REAC13*,REAC14Q[1-2]*}', '{REAC14Q[3-4]*,REAC15*}',
			'{RPSR0[4-9]*,RPSR1[0-1]*,RPSR12Q[1-3]*}', '{RPSR12Q4*,RPSR1[3-5]*}', '{THER0[4-9]*,THER1[0-1]*,THER12Q[1-3]*}',
			'{THER12Q4*,THER1[3-5]*}']

maps = {
	'DEMO1' : [0, 5, 6, 7, 10, 11, 12, 13, 15, 16],
	'DEMO2' : [0, 5, 6, 7, 10, 11, 12, 13, 15, 16],
	'DEMO3' : [0, 4, 5, 7, 10, 11, 12, 13, 15, 16],
	'DEMO4' : [0, 4, 5, 7, 11, 13, 14, 16, 18, 19],
	'DRUG1' : [0, 1, 2, 3, 6, 7, 8, 10],
	'DRUG2' : [0, 2, 3, 4, 7, 10, 11, 13],
	'DRUG3' : [0, 2, 3, 4, 8, 11, 12, 14],
	'INDI1' : [0, 1, 2],
	'INDI2' : [0, 2, 3],
	'OUTC1' : [0, 1],
	'OUTC2' : [0, 2],
	'REAC1' : [0, 1],
	'REAC2' : [0, 2],
	'REAC3' : [0, 2],
	'RPSR1' : [0, 1],
	'RPSR2' : [0, 2],
	'THER1' : [0, 1, 2, 3, 4, 5],
	'THER2' : [0, 2, 3, 4, 5, 6]
}

schemas = {
	'DEMO' : "id event_dt mrf_dt fda_dt mfr_name age age_unit sex weight weight_unit",
	'DRUG' : "id drug_seq role_code drugname dose dechal rechal exp_dt",
	'INDI' : "id drug_seq indi_pt",
	'OUTC' : "id outcome_code",
	'REAC' : "id pt",
	'RPSR' : "id report_code",
	'THER' : "id drug_seq start_dt end_dt duration duration_unit"
}

scfiles = {}
scparts = {}
scelements = {}
scschema = {}
counts = []
for cat, grp in zip(categories, groupings):
	# Read file categories into Spark RDD
	tf = sc.textFile(hdfs_path + "/" + grp)
	# collect headers and remove from input
	header = tf.filter(lambda l: l.startswith("primaryid") or l.startswith("ISR"))
	header.collect()
	tf_nohdr = tf.subtract(header)
	# filter out lines with missing data, where number of fields < greatest field in map
	badlines = tf_nohdr.filter(lambda l: len(l.split("$")) < maps[cat][-1] + 1)
	badlines.collect()
	tf_good = tf_nohdr.subtract(badlines)
	scfiles[cat] = tf_good
	scparts[cat] = tf_good.map(lambda l: l.split("$"))
	scelements[cat] = scparts[cat].map(lambda p: (eval("".join(tuple("p[" + str(i) + "]," for i in maps[cat])))))
	# specify fields via previously defined maps/schemas
	fields = [StructField(field_name, StringType(), True) for field_name in schemas[cat[0:4]].split()]
	schema = StructType(fields)
	scschema[cat] = sqlContext.createDataFrame(scelements[cat], schema)
	counts.append(tf_nohdr.count())

# now combine partitions into original sources (DEMO, DRUG, etc.)
demo = scschema['DEMO1'].unionAll(scschema['DEMO2']).unionAll(scschema['DEMO3']).unionAll(scschema['DEMO4'])
drug = scschema['DRUG1'].unionAll(scschema['DRUG2']).unionAll(scschema['DRUG3'])
indi = scschema['INDI1'].unionAll(scschema['INDI2'])
outc = scschema['OUTC1'].unionAll(scschema['OUTC2'])
reac = scschema['REAC1'].unionAll(scschema['REAC2']).unionAll(scschema['REAC3'])
rpsr = scschema['RPSR1'].unionAll(scschema['RPSR2'])
ther = scschema['THER1'].unionAll(scschema['THER2'])

