# coding=utf-8
from __future__ import print_function
import adodbapi
import re

from pywincc.mssql import mssql, MsSQLException
from pywincc.helper import datetime_to_str, utc_to_local, tic, str_to_date,\
    daterange, date_to_str, datetime_to_str_without_ms, get_next_month,\
    str_to_datetime, remove_timezone, local_time_to_utc
from pywincc.alarm import Alarm, AlarmRecord, alarm_query_builder
from pywincc.tag import Tag, TagRecord, tag_query_builder, plot_tag_records, \
    plot_tag_records2
from pywincc.operator_messages import om_query_builder, OperatorMessageRecord,\
    OperatorMessage
from pywincc.report import generate_alarms_report, operator_messages_report
import pywincc.monkey_patch

from datetime import datetime, timedelta
from dateutil.tz import tzlocal
from pytz import timezone
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import sys
import os

def getdatabase():
   host = os.environ['COMPUTERNAME']+"\WINCC"
   m = mssql(host, '')
   m.connect()
   databases = m.fetch_database_names()
   m.close()
   
   r = re.compile(r"CC_[A-Z]+[\d_]+$")
   wincc_config_databases = filter(r.match, databases)
   return wincc_config_databases[0]


class wincc_conf(mssql):
   def __init__(self, database=None):
      self.host = os.environ['COMPUTERNAME']+"\WINCC"
      self.database = database
      self.conn = None
      self.cursor = None
   def connect(self):
      conn_str = "Provider=%(provider)s; Integrated Security=SSPI; Persist Security Info=False; Initial Catalog=%(database)s;Data Source=%(host)s"
      provider = 'SQLOLEDB.1'
      self.conn = adodbapi.connect(conn_str, provider=provider, host=self.host, database=self.database)
      self.cursor = self.conn.cursor()
   def execute(self, query):
      self.cursor.execute(query)
   def tagid_by_name(self, tagname):
      self.cursor.execute("SELECT TLGTAGID, VARNAME FROM PDE#TAGs WHERE "
                          "VARNAME LIKE '%{name}%'".format(name=tagname))
      for rec in self.cursor.fetchall():
         return rec[0]
   def close(self):
      if self.cursor:
         self.cursor.close()
      if self.conn:
         self.conn.close()

class wincc_tag(mssql):
   def __init__(self, database=None):
      self.host = os.environ['COMPUTERNAME']+"\WINCC"
      self.database = database+"R"
      self.conn = None
      self.cursor = None
   def connect(self):
      conn_str = "Provider=%(provider)s;Catalog=%(database)s;Data Source=%(host)s"
      provider = 'WinCCOLEDBProvider.1'
      self.conn = adodbapi.connect(conn_str, provider=provider, host=self.host, database=self.database)
      self.cursor = self.conn.cursor()
   def execute(self, query):
      self.cursor.execute(query)
   def close(self):
      if self.cursor:
         self.cursor.close()
      if self.conn:
         self.conn.close()


def getdata(wc, wt, tagname, datum):
   toc = tic()
   nextday = (datetime.strptime(datum, '%Y-%m-%d') + timedelta(days=1)).strftime("%Y-%m-%d")
   tagid = wc.tagid_by_name(tagname)
   query = tag_query_builder([tagid], datum+' 00:00:00.000', nextday+' 00:00:00.000',None,"first",False)
   wt.execute(query)
   tquery = round(toc(), 1)
   
   toc = tic()
   num_rows = wt.rowcount()
   result = wt.fetchall()
   #timestamp = [row['timestamp'].replace(tzinfo=tzutc()) for row in result]
   timestamp = [row['timestamp'] for row in result]
   values = [row['variantvalue'] for row in result]
   tcopy = round(toc(), 1)
   
   print("read " + tagname +", got " + str(num_rows) + " records. tquery=" + str(tquery) + " tcopy=" + str(tcopy))
   return timestamp, values

def getalarm(wt, datum):
   nextday = (datetime.strptime(datum, '%Y-%m-%d') + timedelta(days=1)).strftime("%Y-%m-%d")
   query = alarm_query_builder(datum, nextday, '', False, '')
   wt.execute(query)
   alarms = AlarmRecord()
   if wt.rowcount():
      for rec in wt.fetchall():
         date_time = datetime_to_str(utc_to_local(rec['DateTime']))
         alarms.push(Alarm(rec['MsgNr'], rec['State'], date_time,
                           rec['Classname'], rec['Typename'],
                           rec['Text2'], rec['Text1']))
      generate_alarms_report(alarms, datum, nextday, '', '')

if __name__ == "__main__":
   if len(sys.argv) > 1:
      datum = sys.argv[1]
   else:
      datum = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

   print('Datum '+datum)
   
   if not os.path.exists('./temp'):
        os.makedirs('./temp')
   
   tsstart = local_time_to_utc(datetime.strptime(datum, '%Y-%m-%d'))
   tsend = tsstart + timedelta(days=1)

   database = getdatabase()

   wc = wincc_conf(database)
   wt = wincc_tag(database)
   wc.connect()
   wt.connect()

   getalarm(wt, datum)
   
   exP0 = getdata(wc,wt, "exP0", datum)
   exQ0 = getdata(wc,wt, "exQ0", datum)

   exP99 = getdata(wc,wt, "exP99", datum)
   
   exP1 = getdata(wc,wt, "exP1", datum)
   exQ1 = getdata(wc,wt, "exQ1", datum)
   exP2 = getdata(wc,wt, "exP2", datum)
   exQ2 = getdata(wc,wt, "exQ2", datum)
   

   exI1 = getdata(wc,wt, "exI1", datum)
   exI2 = getdata(wc,wt, "exI2", datum)
   exI3 = getdata(wc,wt, "exI3", datum)
   exU1 = getdata(wc,wt, "exU1", datum)
   exU2 = getdata(wc,wt, "exU2", datum)
   exU3 = getdata(wc,wt, "exU3", datum)

   exI4 = getdata(wc,wt, "exI4", datum)
   exI5 = getdata(wc,wt, "exI5", datum)
   exI6 = getdata(wc,wt, "exI6", datum)
   exU4 = getdata(wc,wt, "exU4", datum)
   exU5 = getdata(wc,wt, "exU5", datum)
   exU6 = getdata(wc,wt, "exU6", datum)

   
   ex1S1 = getdata(wc,wt, "ex1S1", datum)
   ex1S2 = getdata(wc,wt, "ex1S2", datum)
   ex1S3 = getdata(wc,wt, "ex1S3", datum)
   ex1S4 = getdata(wc,wt, "ex1S4", datum)
   ex1S5 = getdata(wc,wt, "ex1S5", datum)
   ex1S6 = getdata(wc,wt, "ex1S6", datum)
   ex1S7 = getdata(wc,wt, "ex1S7", datum)
   ex1S8 = getdata(wc,wt, "ex1S8", datum)

   ex2S1 = getdata(wc,wt, "ex2S1", datum)
   ex2S2 = getdata(wc,wt, "ex2S2", datum)
   ex2S3 = getdata(wc,wt, "ex2S3", datum)
   ex2S4 = getdata(wc,wt, "ex2S4", datum)
   ex2S5 = getdata(wc,wt, "ex2S5", datum)
   ex2S6 = getdata(wc,wt, "ex2S6", datum)
   ex2S7 = getdata(wc,wt, "ex2S7", datum)
   ex2S8 = getdata(wc,wt, "ex2S8", datum)

   ex3S1 = getdata(wc,wt, "ex3S1", datum)
   ex3S2 = getdata(wc,wt, "ex3S2", datum)
   ex3S3 = getdata(wc,wt, "ex3S3", datum)
   ex3S4 = getdata(wc,wt, "ex3S4", datum)
   ex3S5 = getdata(wc,wt, "ex3S5", datum)
   ex3S6 = getdata(wc,wt, "ex3S6", datum)
   ex3S7 = getdata(wc,wt, "ex3S7", datum)
   ex3S8 = getdata(wc,wt, "ex3S8", datum)
   
   exT0 = getdata(wc,wt, "exT0", datum)
   exT1 = getdata(wc,wt, "exT1", datum)
   
   ex1T1 = getdata(wc,wt, "ex1T1", datum)
   ex2T1 = getdata(wc,wt, "ex2T1", datum)
   ex3T1 = getdata(wc,wt, "ex3T1", datum)
   
   ex1T2 = getdata(wc,wt, "ex1T2", datum)
   ex2T2 = getdata(wc,wt, "ex2T2", datum)
   ex3T2 = getdata(wc,wt, "ex3T2", datum)
   
   exF1 = getdata(wc,wt, "exF1", datum)
   
   wc.close()
   wt.close()
   
   #Plot AB
   #--------------------------------------------------------------------------------
   print("plot AB")
   plt.rcParams["figure.figsize"] = 11.69,8.27 #DIN A4
   plt.rcParams["font.size"] = 6 #DIN A4
   plt.rcParams["timezone"] = "Europe/Berlin"
   fig = plt.figure(datum)
 
   #Grafik A
   ax1 = fig.add_subplot(211)
   ax1.set_title('AB '+datum)
   ax1.set_xlabel('local time')
   
   ax1.set_ylabel('active power [kW]', color='#000000')
   ax1.set_ylim((-100,100))
   ax1.plot_date(x=exP1[0], y=exP1[1], tz=None, xdate=True, fmt='-', color='red', label='exP1')

   ax2 = ax1.twinx()
   ax2.set_ylabel('reactive power [kVAr]', color='#000000')
   ax2.set_ylim((-100,100))
   ax2.plot_date(x=exQ1[0], y=exQ1[1], tz=None, xdate=True, fmt='-', color='green', label='exQ1')

   ax1.xaxis.set_major_locator(mdates.HourLocator())
   ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M')) #('%y-%m-%d\n%H:%M:%S'))
   ax1.xaxis.set_minor_locator(mdates.MinuteLocator(range(0,59,15)))
   ax1.grid(True)
   ax1.set_xlim(tsstart,tsend)
   
   #Grafik B
   ax3 = fig.add_subplot(212)
   ax3.set_xlabel('local time')

   ax3.set_ylabel('active power [kW]', color='#000000')
   ax3.set_ylim((0,100))
   ax3.plot_date(x=exP2[0], y=exP2[1], tz=None, xdate=True, fmt='-', color='red', label='exP2')

   ax4 = ax3.twinx()
   ax4.set_ylabel('reactive power [kVAr]', color='#000000')
   ax4.set_ylim((-100,100))
   ax4.plot_date(x=exQ2[0], y=exQ2[1], tz=None, xdate=True, fmt='-', color='green', label='exQ2')

   ax3.xaxis.set_major_locator(mdates.HourLocator())
   ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M')) #('%y-%m-%d\n%H:%M:%S'))
   ax3.xaxis.set_minor_locator(mdates.MinuteLocator(range(0,59,15)))
   ax3.grid(True)
   ax3.set_xlim(tsstart,tsend)
   
   ax1.legend(loc='lower left')
   ax2.legend(loc='lower right')
   ax3.legend(loc='lower left')
   ax4.legend(loc='lower right')

   plt.subplots_adjust(left=0.05, right=0.95, top=0.97, bottom=0.05)
   plt.savefig('./temp/plotAB.pdf')
   plt.clf()

   #Plot CD
   #--------------------------------------------------------------------------------
   print("plot CD")
   plt.rcParams["figure.figsize"] = 11.69,8.27 #DIN A4
   plt.rcParams["font.size"] = 6 #DIN A4
   plt.rcParams["timezone"] = "Europe/Berlin"
   fig = plt.figure(datum)
   
   #Grafik C
   ax1 = fig.add_subplot(211)
   ax1.set_title('CD '+datum)
   ax1.set_xlabel('local time')
   
   ax1.set_ylabel('voltage [Veff]', color='#000000')
   ax1.set_ylim((300,500))
   ax1.plot_date(x=exU1[0], y=exU1[1], tz=None, xdate=True, fmt='-', color='brown', label='exU1')
   ax1.plot_date(x=exU2[0], y=exU2[1], tz=None, xdate=True, fmt='-', color='black', label='exU2')
   ax1.plot_date(x=exU3[0], y=exU3[1], tz=None, xdate=True, fmt='-', color='gray', label='exU3')

   ax2 = ax1.twinx()
   ax2.set_ylabel('current [A]', color='#000000')
   ax2.set_ylim((0,100))
   ax2.plot_date(x=exI1[0], y=exI1[1], tz=None, xdate=True, fmt='-', color='red', label='exI1')
   ax2.plot_date(x=exI2[0], y=exI2[1], tz=None, xdate=True, fmt='-', color='blue', label='exI2')
   ax2.plot_date(x=exI3[0], y=exI3[1], tz=None, xdate=True, fmt='-', color='green', label='exI3')

   ax1.xaxis.set_major_locator(mdates.HourLocator())
   ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M')) #('%y-%m-%d\n%H:%M:%S'))
   ax1.xaxis.set_minor_locator(mdates.MinuteLocator(range(0,59,15)))
   ax1.grid(True)
   ax1.set_xlim(tsstart,tsend)
   
   #Grafik D
   ax3 = fig.add_subplot(212)
   ax3.set_xlabel('local time')

   ax3.set_ylabel('voltage [Veff]', color='#000000')
   ax3.set_ylim((300,500))
   ax3.plot_date(x=exU4[0], y=exU4[1], tz=None, xdate=True, fmt='-', color='brown', label='exU4')
   ax3.plot_date(x=exU5[0], y=exU5[1], tz=None, xdate=True, fmt='-', color='black', label='exU5')
   ax3.plot_date(x=exU6[0], y=exU6[1], tz=None, xdate=True, fmt='-', color='gray', label='exU6')

   ax4 = ax3.twinx()
   ax4.set_ylabel('current [A]', color='#000000')
   ax4.set_ylim((0,100))
   ax4.plot_date(x=exI4[0], y=exI4[1], tz=None, xdate=True, fmt='-', color='red', label='exI4')
   ax4.plot_date(x=exI5[0], y=exI5[1], tz=None, xdate=True, fmt='-', color='blue', label='exI5')
   ax4.plot_date(x=exI6[0], y=exI6[1], tz=None, xdate=True, fmt='-', color='green', label='exI6')

   ax3.xaxis.set_major_locator(mdates.HourLocator())
   ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M')) #('%y-%m-%d\n%H:%M:%S'))
   ax3.xaxis.set_minor_locator(mdates.MinuteLocator(range(0,59,15)))
   ax3.grid(True)
   ax3.set_xlim(tsstart,tsend)

   ax1.legend(loc='lower left')
   ax2.legend(loc='lower right')
   ax3.legend(loc='lower left')
   ax4.legend(loc='lower right')

   plt.subplots_adjust(left=0.05, right=0.95, top=0.97, bottom=0.05)
   plt.savefig('./temp/plotCD.pdf')
   plt.clf()   
   
   #Plot EF
   #--------------------------------------------------------------------------------
   print("plot EF")
   plt.rcParams["figure.figsize"] = 11.69,8.27 #DIN A4
   plt.rcParams["font.size"] = 6 #DIN A4
   plt.rcParams["timezone"] = "Europe/Berlin"
   fig = plt.figure(datum)
   
   #Grafik E
   ax1 = fig.add_subplot(211)
   ax1.set_title('EF '+datum)
   ax1.set_xlabel('local time')
   
   ax1.set_ylabel('State [%]', color='#000000')
   ax1.set_ylim((0,100))
   ax1.plot_date(x=ex1S1[0], y=ex1S1[1], tz=None, xdate=True, fmt='-', color='darkmagenta', label='Container 1')
   ax1.plot_date(x=ex1S2[0], y=ex1S2[1], tz=None, xdate=True, fmt='-', color='darkmagenta')
   ax1.plot_date(x=ex1S3[0], y=ex1S3[1], tz=None, xdate=True, fmt='-', color='darkmagenta')
   ax1.plot_date(x=ex1S4[0], y=ex1S4[1], tz=None, xdate=True, fmt='-', color='darkmagenta')
   ax1.plot_date(x=ex1S5[0], y=ex1S5[1], tz=None, xdate=True, fmt='-', color='darkmagenta')
   ax1.plot_date(x=ex1S6[0], y=ex1S6[1], tz=None, xdate=True, fmt='-', color='darkmagenta')
   ax1.plot_date(x=ex1S7[0], y=ex1S7[1], tz=None, xdate=True, fmt='-', color='darkmagenta')
   ax1.plot_date(x=ex1S8[0], y=ex1S8[1], tz=None, xdate=True, fmt='-', color='darkmagenta')

   ax1.plot_date(x=ex2S1[0], y=ex2S1[1], tz=None, xdate=True, fmt='-', color='darkorange', label='Container 2')
   ax1.plot_date(x=ex2S2[0], y=ex2S2[1], tz=None, xdate=True, fmt='-', color='darkorange')
   ax1.plot_date(x=ex2S3[0], y=ex2S3[1], tz=None, xdate=True, fmt='-', color='darkorange')
   ax1.plot_date(x=ex2S4[0], y=ex2S4[1], tz=None, xdate=True, fmt='-', color='darkorange')
   ax1.plot_date(x=ex2S5[0], y=ex2S5[1], tz=None, xdate=True, fmt='-', color='darkorange')
   ax1.plot_date(x=ex2S6[0], y=ex2S6[1], tz=None, xdate=True, fmt='-', color='darkorange')
   ax1.plot_date(x=ex2S7[0], y=ex2S7[1], tz=None, xdate=True, fmt='-', color='darkorange')
   ax1.plot_date(x=ex2S8[0], y=ex2S8[1], tz=None, xdate=True, fmt='-', color='darkorange')

   ax1.plot_date(x=ex3S1[0], y=ex3S1[1], tz=None, xdate=True, fmt='-', color='forestgreen', label='Container 3')
   ax1.plot_date(x=ex3S2[0], y=ex3S2[1], tz=None, xdate=True, fmt='-', color='forestgreen')
   ax1.plot_date(x=ex3S3[0], y=ex3S3[1], tz=None, xdate=True, fmt='-', color='forestgreen')
   ax1.plot_date(x=ex3S4[0], y=ex3S4[1], tz=None, xdate=True, fmt='-', color='forestgreen')
   ax1.plot_date(x=ex3S5[0], y=ex3S5[1], tz=None, xdate=True, fmt='-', color='forestgreen')
   ax1.plot_date(x=ex3S6[0], y=ex3S6[1], tz=None, xdate=True, fmt='-', color='forestgreen')
   ax1.plot_date(x=ex3S7[0], y=ex3S7[1], tz=None, xdate=True, fmt='-', color='forestgreen')
   ax1.plot_date(x=ex3S8[0], y=ex3S8[1], tz=None, xdate=True, fmt='-', color='forestgreen')
   
   ax1.xaxis.set_major_locator(mdates.HourLocator())
   ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M')) #('%y-%m-%d\n%H:%M:%S'))
   ax1.xaxis.set_minor_locator(mdates.MinuteLocator(range(0,59,15)))
   ax1.grid(True)
   ax1.set_xlim(tsstart,tsend)

   #Grafik F
   ax3 = fig.add_subplot(212)
   ax3.set_xlabel('local time')
   ax3.xaxis.set_major_locator(mdates.HourLocator())
   ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M')) #('%y-%m-%d\n%H:%M:%S'))
   ax3.xaxis.set_minor_locator(mdates.MinuteLocator(range(0,59,15)))

   ax3.set_ylabel(u'temperature [°C]', color='#000000')
   ax3.set_ylim((-20,50))

   ax3.plot_date(x=ex1T2[0], y=ex1T2[1], tz=None, xdate=True, fmt='-', color='darkmagenta')
   ax3.plot_date(x=ex2T2[0], y=ex2T2[1], tz=None, xdate=True, fmt='-', color='darkorange')
   ax3.plot_date(x=ex3T2[0], y=ex3T2[1], tz=None, xdate=True, fmt='-', color='forestgreen')


   ax3.plot_date(x=exT0[0], y=exT0[1], tz=None, xdate=True, fmt='-', color='gray', label='exT0')
   ax3.plot_date(x=exT1[0], y=exT1[1], tz=None, xdate=True, fmt='-', color='gold', label='exT1')
   ax3.plot_date(x=ex1T1[0], y=ex1T1[1], tz=None, xdate=True, fmt='-', color='darkmagenta', label='Container 1')
   ax3.plot_date(x=ex2T1[0], y=ex2T1[1], tz=None, xdate=True, fmt='-', color='darkorange', label='Container 2')
   ax3.plot_date(x=ex3T1[0], y=ex3T1[1], tz=None, xdate=True, fmt='-', color='forestgreen', label='Container 3')
   
   ax3.grid(True)
   ax3.set_xlim(tsstart,tsend)
   
   ax1.legend(loc='lower left')
   ax3.legend(loc='lower left')

   plt.subplots_adjust(left=0.05, right=0.95, top=0.97, bottom=0.05)
   #plt.savefig(datum+'-plot2.png',dpi=300)
   plt.savefig('./temp/plotEF.pdf')
   plt.clf()
   
   #Plot G
   #--------------------------------------------------------------------------------
   print("plot GH")
   plt.rcParams["figure.figsize"] = 11.69,8.27 #DIN A4
   plt.rcParams["font.size"] = 6 #DIN A4
   plt.rcParams["timezone"] = "Europe/Berlin"
   fig = plt.figure(datum)
 
   #Grafik G
   ax1 = fig.add_subplot(211)
   ax1.set_title('GH '+datum)
   ax1.set_xlabel('local time')
   ax1.xaxis.set_major_locator(mdates.HourLocator())
   ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M')) #('%y-%m-%d\n%H:%M:%S'))
   ax1.xaxis.set_minor_locator(mdates.MinuteLocator(range(0,59,15)))
   
   ax1.set_ylabel('frequency [Hz]', color='#000000')
   ax1.set_ylim((0,100))
   ax1.plot_date(x=exF1[0], y=exF1[1], tz=None, xdate=True, fmt='-', color='g', label='exF1')
   ax1.grid(True)
   ax1.set_xlim(tsstart,tsend)

   #Grafik H
   ax3 = fig.add_subplot(212)
   ax3.set_xlabel('local time')

   ax3.set_ylabel('active power [kW]', color='#000000')
   ax3.set_ylim((-100,100))
   ax3.plot_date(x=exP0[0], y=exP0[1], tz=None, xdate=True, fmt='-', color='red', label='exP0')
   ax3.plot_date(x=exP99[0], y=exP99[1], tz=None, xdate=True, fmt='-', color='darkred', label='exP99')

   ax4 = ax3.twinx()
   ax4.set_ylabel('reactive power [kVAr]', color='#000000')
   ax4.set_ylim((-100,100))
   ax4.plot_date(x=exQ0[0], y=exQ0[1], tz=None, xdate=True, fmt='-', color='green', label='exQ0')

   ax3.xaxis.set_major_locator(mdates.HourLocator())
   ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M')) #('%y-%m-%d\n%H:%M:%S'))
   ax3.xaxis.set_minor_locator(mdates.MinuteLocator(range(0,59,15)))
   ax3.grid(True)
   ax3.set_xlim(tsstart,tsend)
   
   ax1.legend(loc='lower left')
   ax3.legend(loc='lower left')
   ax4.legend(loc='lower right')

   plt.subplots_adjust(left=0.05, right=0.95, top=0.97, bottom=0.05)
   plt.savefig('./temp/plotG.pdf')
   plt.clf()
   
   if not os.path.exists('./plots'):
        os.makedirs('./plots')
   
   print("merge")
   os.system("pdftk .\\temp\\*.pdf cat output .\\plots\\"+datum+"-plot.pdf");