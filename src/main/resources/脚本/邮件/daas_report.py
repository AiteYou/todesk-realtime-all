#!/home/bigdata/module/miniconda3/envs/superset/bin/python3.7
# coding: utf-8

# In[1]:


import pymysql
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart
from prettytable import PrettyTable
import datetime


# In[2]:


# pip install prettytable


# In[118]:


class DbConnection:

    def __init__(self, my_host, my_user, my_pass, db_name):
        self.db = pymysql.connect(host=my_host, user=my_user, password=my_pass, database=db_name,
                     cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.db.cursor()

    def execute(self, sql):
        print ("executing SQL: " + sql)
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        return results



# In[142]:

def getYesterday():
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    return yesterday

def get_pretty_table(sql_result, target_fields_name, target_fields):
    tabular_table = PrettyTable()
    tabular_table.field_names = target_fields_name
    # tabular_table.add_row(["Jill", "Smith", 50])

    # sql = """
    #     select * from ads_android_first_frame_total where dt='%s' and platform='android' order by version desc 
    #     """ % dt
    # cursor.execute(sql)
    # results = cursor.fetchall()
    for item in sql_result:
        row_fields = [item[target_field] for target_field in target_fields]
        tabular_table.add_row(row_fields)
    return tabular_table.get_html_string()


def get_html(content):
    paragraph = ""
    for key, value in content.items():
        paragraph = paragraph + """%s<br>
           %s <br>""" % (key, value)
    html_str = """        <html>
            <head>
            <style>
                table, th, td {
                    border: 1px solid black;
                    border-collapse: collapse;
                }
                th, td {
                    padding: 5px;
                    text-align: left;    
                }    
            </style>
            </head>
        <body>
        <p>
        %s
        </p>
        </body>
        </html>
        """ % (paragraph)
    return MIMEText(html_str, 'html')
    

def send_email(my_sender, my_email_pass, recipients, content, report_date):
    try:
        msg = MIMEMultipart('alternative')
        text = ", ".join(recipients)
        msg['From'] = formataddr(("刘超", my_sender))  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
#         msg['To'] = formataddr(("", text))  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        msg['To'] = ", ".join(recipients)  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        msg['Subject'] = "daas后端每日报表: %s" % report_date  # 邮件的主题，也可以说是标题
        msg.attach(content)
        server = smtplib.SMTP_SSL("smtp.zuler.io", 465)  # 发件人邮箱中的SMTP服务器，端口是25
        server.login(my_sender, my_email_pass)  # 括号中对应的是发件人邮箱账号、邮箱密码
        server.sendmail(my_sender, recipients, msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
        server.quit()  # 关闭连接
        print("邮件发送成功")
    except Exception as e:  # 如果 try 中的语句没有执行，则会执行下面的 ret=False
        ret = False
        print (e)
        print("邮件发送失败")


# In[143]:

def get_ads_daas_total_session_day_query(date):
    return """
    SELECT sum(handshake_success_count) handshake_success_count,sum(connection_success_count) connection_success_count,sum(connection_count) connection_count,
    cast(SUM(handshake_success_count) / SUM(connection_count) * 100 as DECIMAL(16,2)) handshake_success_rate,
    cast(SUM(connection_success_count) / SUM(connection_count) * 100 as DECIMAL(16,2)) session_rate
    FROM ads_daas_serv_session_day
    WHERE dt='%s'
    """ % (date)

def get_ads_daas_licode_total_session_day_query(date):
    return """
    SELECT ec_success_count,auth_success_count,ec_success_rate
    FROM ads_daas_licode_handshake
    WHERE dt='%s' and type=1 order by type
    """ % (date)

def get_ads_daas_handshake_query(date):
    return """
    SELECT type,handshake_success_count,connection_count,handshake_success_rate
    FROM ads_daas_handshake
    WHERE dt='%s' order by type
    """ % (date)
def get_ads_daas_forward_failed_total_query(date):
    return """
select 
        t1.*,cast(cnt/total_connection_count*100 as DECIMAL(16,2)) connection_rate
from
(
	select dt,reason,cnt from ads_daas_forward_failed_total 
        WHERE dt='%s'
)t1
left join (
        select dt,sum(connection_count) total_connection_count  FROM ads_daas_client_session_day
        WHERE dt='%s'  group by dt
)t2
on t1.dt=t2.dt
order by cnt desc
    """ % (date,date)

def get_ads_licode_daas_handshake_query(date):
    return """
    SELECT if(type=2,'presenter','controller') type,ec_success_count,auth_success_count,ec_success_rate
    FROM ads_daas_licode_handshake
    WHERE dt='%s' and type in (2,3) order by type
    """ % (date)

def get_ads_dass_connection_time_query(date):
    return """
    SELECT sum(client_link_time) link_time ,sum(ec_success_count) connection_success_count,cast(sum(client_link_time)/sum(ec_success_count) as decimal(16,0)) duration_average FROM ads_daas_client_session_day
    WHERE dt='%s'
    """ % (date)

def get_ads_daas_client_session_day_query(date):
    return """
select 
        t1.*,cast(connection_count/total_connection_count*100 as DECIMAL(16,1)) connection_rate
from
(
        SELECT dt,sclient,sum(handshake_success_count) handshake_success_count,sum(connection_success_count) connection_success_count,sum(connection_count) connection_count,cast(100.00 * sum(handshake_success_count)/sum(connection_count) as decimal(16,2)) handshake_success_rate,cast(100.00 * sum(connection_success_count)/sum(connection_count) as decimal(16,2)) session_rate,sum(client_link_time) total_duration,cast(sum(client_link_time)/sum(ec_success_count) as decimal(16,0)) duration_average FROM ads_daas_client_session_day
        WHERE dt='%s'
	group by dt,sclient
)t1
left join (
        select dt,sum(connection_count) total_connection_count  FROM ads_daas_client_session_day
        WHERE dt='%s'  group by dt
)t2
on t1.dt=t2.dt
order by connection_count desc 
    """ % (date,date)

def get_ads_daas_serv_session_day_query(date):
    return """
    select 
	t1.*,cast(connection_count/total_connection_count*100 as DECIMAL(16,1)) connection_rate
from
(
	SELECT * FROM ads_daas_serv_session_day
	WHERE dt='%s'
)t1
left join (
	select dt,sum(connection_count) total_connection_count  FROM ads_daas_serv_session_day
	WHERE dt='%s'  group by dt
)t2
on t1.dt=t2.dt
order by connection_count desc 
    """ % (date,date)

def get_ads_daas_isp_handshake_query(date):
    return """
    select 
        t1.*,cast(connection_count/total_connection_count*100 as DECIMAL(16,1)) connection_rate
from
(
        SELECT * FROM ads_daas_isp_handshake
        WHERE dt='%s'
)t1
left join (
        select dt,sum(connection_count) total_connection_count  FROM ads_daas_isp_handshake
        WHERE dt='%s'  group by dt
)t2
on t1.dt=t2.dt
order by connection_count desc 
    """ % (date,date)

def get_ads_daas_scountry_session_day_query(date):
    return """
    select 
	t1.*,cast(connection_count/total_connection_count*100 as DECIMAL(16,2)) connection_rate
from
(
	SELECT dt,scountry,sum(handshake_success_count) handshake_success_count,sum(connection_success_count) connection_success_count,sum(connection_count) connection_count,
    cast(SUM(handshake_success_count) / SUM(connection_count) * 100 as DECIMAL(16,2)) handshake_success_rate,
    cast(SUM(connection_success_count) / SUM(connection_count) * 100 as DECIMAL(16,2)) session_rate
    FROM ads_daas_country_session_day
    WHERE dt='%s'
    group by dt,scountry
)t1
left join (
	select dt,sum(connection_count) total_connection_count  FROM ads_daas_country_session_day
	WHERE dt='%s'  group by dt
)t2
on t1.dt=t2.dt
order by connection_count desc 
    """ % (date,date)

def get_ads_daas_dcountry_session_day_query(date):
    return """
    select 
	t1.*,cast(connection_count/total_connection_count*100 as DECIMAL(16,2)) connection_rate
from
(
	SELECT dt,dcountry,sum(handshake_success_count) handshake_success_count,sum(connection_success_count) connection_success_count,sum(connection_count) connection_count,
    cast(SUM(handshake_success_count) / SUM(connection_count) * 100 as DECIMAL(16,2)) handshake_success_rate,
    cast(SUM(connection_success_count) / SUM(connection_count) * 100 as DECIMAL(16,2)) session_rate
    FROM ads_daas_country_session_day
    WHERE dt='%s'
    group by dt,dcountry
)t1
left join (
	select dt,sum(connection_count) total_connection_count  FROM ads_daas_country_session_day
	WHERE dt='%s'  group by dt
)t2
on t1.dt=t2.dt
order by connection_count desc 
    """ % (date,date)

def get_ads_daas_sprovince_session_day_query(date):
    return """
    select 
	t1.*,cast(connection_count/total_connection_count*100 as DECIMAL(16,1)) connection_rate
from
(
	SELECT * FROM ads_daas_sprovince_session_day
	WHERE dt='%s'
)t1
left join (
	select dt,sum(connection_count) total_connection_count  FROM ads_daas_sprovince_session_day
	WHERE dt='%s'  group by dt
)t2
on t1.dt=t2.dt
order by connection_count desc 
    """ % (date,date)


def get_ads_daas_dprovince_session_day_query(date):
    return """
   select 
        t1.*,cast(connection_count/total_connection_count*100 as DECIMAL(16,1)) connection_rate
from
(
        SELECT * FROM ads_daas_dprovince_session_day
        WHERE dt='%s'
)t1
left join (
        select dt,sum(connection_count) total_connection_count  FROM ads_daas_dprovince_session_day
        WHERE dt='%s'  group by dt
)t2
on t1.dt=t2.dt
order by connection_count desc 
    """ % (date,date)

def gat_ads_dass_connect_fail_total_su(date):
	return """
	select id,handshake_success_count,handshake_fail_count,newconnect_count,handshake_success_rate,connect_rate from ads_dass_connect_fail_total
	WHERE dt='%s' and flag='su' order by handshake_fail_count desc """ % (date)

def gat_ads_dass_connect_fail_total_du(date):
        return """
        select id,handshake_success_count,handshake_fail_count,newconnect_count,handshake_success_rate,connect_rate from ads_dass_connect_fail_total
        WHERE dt='%s' and flag='du' order by handshake_fail_count desc """ % (date)

tables = {
    "ads_daas_total_session_day" : {
        "handshake_success_count" : "握手成功数",
        "connection_success_count" : "鉴权成功数",
        "connection_count" : "发起连接数",
        "handshake_success_rate" : "握手成功率",
        "session_rate" : "鉴权成功率"
    },
    "ads_daas_licode_total_session_day" : {
        "ec_success_count" : "ec成功数",
        "auth_success_count" : "鉴权成功数",
        "ec_success_rate" : "ec成功率"
    },
    "ads_daas_handshake" : {
        "type": "控制方",
        "handshake_success_count": "握手成功数",
        "connection_count": "发起连接数",
        "handshake_success_rate": "握手成功率"
    },
    "ads_daas_forward_failed_total" : {
	"reason": "原因",
	"cnt": "数量",
        "connection_rate": "占比"
    },
    "ads_daas_licode_handshake" : {
        "type": "控制方",
	"ec_success_count" : "ec成功数",
        "auth_success_count" : "鉴权成功数",
        "ec_success_rate" : "ec成功率"
    },
    "ads_daas_conn_day" : {
        "link_time" : "连接总时长",
        "connection_success_count": "会话成功数",
        "duration_average": "会话平均时长"
    },
    "ads_daas_client_session_day" : {
        "sclient" : "平台",
        "handshake_success_count" : "握手成功数",
        "connection_success_count" : "鉴权成功数",
        "connection_count" : "发起连接数",
        "handshake_success_rate" : "握手成功率",
        "session_rate" : "鉴权成功率",
        "total_duration" : "会话时长",
        "duration_average" : "平均会话时长",
        "connection_rate" : "占比"
    },
    "ads_daas_serv_session_day" : {
        "relay_server_name" : "机房",
        "handshake_success_count" : "握手成功数",
        "connection_success_count" : "鉴权成功数",
        "connection_count" : "发起连接数",
        "handshake_success_rate" : "握手成功率",
        "session_rate" : "鉴权成功率",
        "connection_rate" : "占比"
    },
    "ads_daas_isp_handshake" : {
        "sisp" : "主isp",
	"disp" : "被isp",
        "handshake_success_count" : "握手成功数",
        "connection_success_count" : "鉴权成功数",
        "connection_count" : "发起连接数",
        "handshake_success_rate" : "握手成功率",
        "session_rate" : "鉴权成功率",
        "connection_rate" : "占比"
    },
    "ads_daas_scountry_session_day" : {
        "scountry": "主控国家",
        "handshake_success_count": "握手成功数",
        "connection_success_count": "鉴权成功数",
        "connection_count": "发起连接数",
        "handshake_success_rate": "握手成功率",
        "session_rate": "鉴权成功率",
        "connection_rate" : "占比"
    },
    "ads_daas_dcountry_session_day" : {
        "dcountry": "被控国家",
        "handshake_success_count": "握手成功数",
        "connection_success_count": "鉴权成功数",
        "connection_count": "发起连接数",
        "handshake_success_rate": "握手成功率",
        "session_rate": "鉴权成功率",
        "connection_rate" : "占比"
    },
    "ads_daas_sprovince_session_day" : {
        "sprovince": "主控省份",
        "handshake_success_count": "握手成功数",
        "connection_success_count": "鉴权成功数",
        "connection_count": "发起连接数",
        "handshake_success_rate": "握手成功率",
        "session_rate": "鉴权成功率",
        "connection_rate" : "占比"
    },
    "ads_daas_dprovince_session_day" : {
        "dprovince": "机房",
        "handshake_success_count": "握手成功数",
        "connection_success_count": "鉴权成功数",
        "connection_count": "发起连接数",
        "handshake_success_rate": "握手成功率",
        "session_rate": "鉴权成功率",
        "connection_rate" : "占比"
    },
    "ads_dass_connect_fail_total" : {
	"id" : "id",
	"handshake_success_count" :  "握手成功数",
	"handshake_fail_count" : "握手失败数",
	"newconnect_count" : "发起连接数",
	"handshake_success_rate" : "成功率",
	"connect_rate" : "失败占比"
    }
}

class Table:
    def __init__(self, name, schema, queries):
        self.name = name
        self.schema = schema
        self.queries = queries


class Table_collection:

    def __init__(self):
        self.schema = []
        self.query = []

def get_query(date):
    return {
	    "会话时长(秒)" : get_ads_dass_connection_time_query(date),
	    "forward连通率(总)" : get_ads_daas_total_session_day_query(date),
            "forward连通率(控制方)" : get_ads_daas_handshake_query(date),
            "forward被控失败归因" : get_ads_daas_forward_failed_total_query(date),
            "forward连通率(平台)" : get_ads_daas_client_session_day_query(date),
	    "forward连通率(机房)" : get_ads_daas_serv_session_day_query(date),
            "forward连通率(被控机房)" : get_ads_daas_dprovince_session_day_query(date),
	    "forward连通率(运营商)" : get_ads_daas_isp_handshake_query(date),
            "forward连通率(主控国家)" : get_ads_daas_scountry_session_day_query(date),
            "forward连通率(被控国家)" : get_ads_daas_dcountry_session_day_query(date),
            "forward连通率(主控省份)" : get_ads_daas_sprovince_session_day_query(date),
            "licode连通率(总)" : get_ads_daas_licode_total_session_day_query(date),
            "licode连通率(控制方)" : get_ads_licode_daas_handshake_query(date),
	    "主控握手失败设备top10" : gat_ads_dass_connect_fail_total_su(date),
	    "被控握手失败设备top10" : gat_ads_dass_connect_fail_total_du(date),
           }

def get_metrics():
    return {
	    "会话时长(秒)" : tables["ads_daas_conn_day"],
            "forward连通率(总)" : tables["ads_daas_total_session_day"],
            "forward连通率(控制方)" : tables["ads_daas_handshake"],
            "forward被控失败归因" : tables["ads_daas_forward_failed_total"],
            "forward连通率(平台)" : tables["ads_daas_client_session_day"],
            "forward连通率(机房)" : tables["ads_daas_serv_session_day"],
            "forward连通率(被控机房)" : tables["ads_daas_dprovince_session_day"],
	    "forward连通率(运营商)" : tables["ads_daas_isp_handshake"],
            "forward连通率(主控国家)" : tables["ads_daas_scountry_session_day"],
            "forward连通率(被控国家)" : tables["ads_daas_dcountry_session_day"],
            "forward连通率(主控省份)" : tables["ads_daas_sprovince_session_day"],
            "licode连通率(总)" : tables["ads_daas_licode_total_session_day"],
            "licode连通率(控制方)" : tables["ads_daas_licode_handshake"],
	   "主控握手失败设备top10" : tables["ads_dass_connect_fail_total"],
	   "被控握手失败设备top10" : tables["ads_dass_connect_fail_total"]
           }


# In[146]:


if __name__ == '__main__':
    my_host = "113.31.158.46"
    my_user = "rtc"
    my_pass = "46%S!vcTk&8Cqr6#"
    db_name = "todesk_report"

    db_connection = DbConnection(my_host, my_user, my_pass, db_name)
    date = getYesterday()
    report_queries = get_query(date)
    report_metrics = get_metrics()
    entry_to_content = {}
    for report_entry, query in report_queries.items():
        try:
            sql_result = db_connection.execute(query)
            if (len(sql_result) == 0):
                raise Exception("Error: sql result is empty running query: " + query)
            report_entry_metrics = report_metrics[report_entry]
            target_fields_name = report_entry_metrics.values()
            target_fields = report_entry_metrics.keys()
            entry_to_content[report_entry] = get_pretty_table(sql_result, target_fields_name, target_fields)
        except Exception as e: 
            print(str(e))

    email_sender = 'liuchao@zuler.io'
    email_sender_pass = 'Mk32ha4PDqJzm4C7'
    #recipients = ['liuchao@zuler.io']
    recipients = ['backend@zuler.io', 'media_team@zuler.io', 'platform@zuler.io', 'zhumin@zuler.io','td_product@zuler.io','zhangyi@zuler.io','zhuxiangnan@zuler.io']
    html_content = get_html(entry_to_content)
    send_email(email_sender, email_sender_pass, recipients, html_content, date)

