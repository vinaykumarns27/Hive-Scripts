

from datetime import datetime, timedelta
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import abs, col, date_add, date_format, lit, round, unix_timestamp, when
from pyspark.sql.types import StringType, StructField, StructType

def getDateDF(sparkSess):
    dateRDD = sparkSess.sparkContext.textFile("/dimension/common/tt_dw_date_dim_with_36_month_week_filter", use_unicode=True) \
        .map(lambda x: x.split("\001")) \
        .map(lambda x: Row(the_date = x[1], the_date2 = x[2]))
    return sparkSess.createDataFrame(dateRDD)

def getThreeYearStartDateFromTable(sparkSess):
    dateDF = getDateDF(sparkSess)
    startDate = dateDF.select(date_format(dateDF.the_date, "yyyy-MM-dd")).orderBy(dateDF.the_date, ascending=True).limit(1).collect()[0][0]
    return startDate

def getThreeYearEndDateFromTable(sparkSess):
    dateDF = getDateDF(sparkSess)
    endDate = dateDF.select(date_format(date_add(dateDF.the_date, 1), "yyyy-MM-dd")).orderBy(dateDF.the_date, ascending=False).limit(1).collect()[0][0]
    return endDate

def getOneHundredEightyDayStartDateFromTable(sparkSess):
    date_current_from_table = datetime.strptime(getThreeYearEndDateFromTable(sparkSess), '%Y-%m-%d')
    bh_date_ohe_days_ago = date_current_from_table - timedelta(days=6*30)
    date_ohe_days_ago = bh_date_ohe_days_ago.strftime('%Y-%m-%d')

    dateDF = getDateDF(sparkSess)
    endDate = dateDF.filter(dateDF.the_date >= date_ohe_days_ago).select(date_format(date_add(dateDF.the_date, 1), "yyyy-MM-dd")).orderBy(dateDF.the_date, ascending=True).limit(1).collect()[0][0]
    return endDate

# equivalent to /dimension/common/tt_dw_date_dim_with_36_month_week_filter
# calculate start date, earlist complete week including 1st of the month 3 years in the past, start on Sunday
def getThreeYearStartDate():
    now = datetime.now()
    past = now.replace(year=now.year-3, day=1)
    # 1 (Mon) to 7 (Sun)
    dow = past.isoweekday()
    if (dow == 7):
        dow = 0
    return (past + timedelta(days=-dow)).strftime("%Y-%m-%d")

# calculate end date, most recent complete week, end before Sunday
def getThreeYearEndDate():
    now = datetime.now()
    dow = now.isoweekday()
    if (dow == 7):
        dow = 0
    return (now + timedelta(days=-dow)).strftime("%Y-%m-%d")

# all StructFields are nullable in returned StructType
def getSchemaFromString(str, delimiter):
    return StructType([StructField(name, StringType(), True) for name in str.split(delimiter)])

# handle null column that has empty string or \N
def isNotNullCol(col):
    return col.isNotNull() & (col != 'null') & (col != '\N')

# make sure NULL is really NULL
# this (list comprehension)) will apply to every dataframe column
# note: for loop with withColumn seems to work with a small number of columns only.
def nullifyDFNullValue(inDF):
    return inDF.select(*[nullifyNullableCol(x).alias(x) for x in inDF.columns])

# helper common method
def nullifyNullableCol(colName):
    return when((col(colName) != "null") & (col(colName) != "\N"), col(colName)).otherwise(lit(None))

# calculate cycle time in day(s) within x day threshold
def getCycleTime(t1, t2, threshold = 0):
    return round(getCycleTimeHour(t1, t2, threshold) / 24.0, 2)

def getCycleTimeHour(t1, t2, threshold = 0):
    if (threshold != 0):
        return (when(abs((unix_timestamp(t1) - unix_timestamp(t2)) / 3600.0 / 24.0) <= threshold, round((unix_timestamp(t1) - unix_timestamp(t2)) / 3600.0, 2)))
    return round((unix_timestamp(t1) - unix_timestamp(t2)) / 3600.0, 2)