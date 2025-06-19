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
        msg['Subject'] = "todesk后端每日报表: %s" % report_date  # 邮件的主题，也可以说是标题
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

def get_forward_connection_success_rate_query(date):
    return """
    SELECT sum(handshake_success_count) handshake_success_count,sum(connection_success_count) connection_success_count,sum(connection_count) connection_count,
    cast(SUM(handshake_success_count) / SUM(connection_count) * 100 as DECIMAL(16,2)) handshake_success_rate,
    cast(SUM(connection_success_count) / SUM(connection_count) * 100 as DECIMAL(16,2)) session_rate
    FROM ads_session_day
    WHERE dt='%s'
    """ % (date)


def get_forward_user_connection_success_rate_query(date):
    return """
    SELECT enterprise,sum(handshake_success_count) handshake_success_count,sum(connection_success_count) connection_success_count,sum(connection_count) connection_count,
    cast(SUM(handshake_success_count) / SUM(connection_count) * 100 as DECIMAL(16,2)) handshake_success_rate,
    cast(SUM(connection_success_count) / SUM(connection_count) * 100 as DECIMAL(16,2)) session_rate
    FROM ads_session_day
    WHERE dt='%s'
    group by  enterprise
    """ % (date)

def get_forward_reconnect_connection_success_rate_query(date):
    return """
    SELECT case when reconnect = '0' then '否' when reconnect = '1' then '是' else 'other' end reconnect,sum(handshake_success_count) handshake_success_count,sum(connection_success_count) connection_success_count,sum(connection_count) connection_count,
    cast(SUM(handshake_success_count) / SUM(connection_count) * 100 as DECIMAL(16,2)) handshake_success_rate,
    cast(SUM(connection_success_count) / SUM(connection_count) * 100 as DECIMAL(16,2)) session_rate
    FROM ads_session_day
    WHERE dt='%s'
    group by  case when reconnect = '0' then '否' when reconnect = '1' then '是' else 'other' end
    """ % (date)

def get_ads_ec_total_query(date):
    return """SELECT sum(succcess_count) as 'succcess_count',sum(connection_count) as 'connection_count',cast(sum(succcess_count)/sum(connection_count) *100 as DECIMAL(16,2)) as 'succcess_rate'
from ads_ec_total where dt='%s' """ % (date);

def get_ads_ea_total(date):
    return """
        select
            sum(handshake_success_count) as 'succcess_count',sum(connection_count) as 'connection_count',cast(sum(handshake_success_count)/sum(connection_count) *100 as DECIMAL(16,2)) as 'succcess_rate'
        from ads_ea_hour_isp_total
        where dt='%s'
        """ % date

def get_forward_connection_time_c_query(date):
    return get_forward_connection_time_query(date, "consumer")

def get_forward_connection_time_b_query(date):
    return get_forward_connection_time_query(date, "business")

def get_forward_connection_time_query(date, enterprise):
    return """
    SELECT * FROM ads_conn_duration
    WHERE dt='%s'
    AND enterprise='%s'
    """ % (date, enterprise)

def get_licode_connection_time_query(date):
    return """
    SELECT sum(conn_time_total) conn_time_total,sum(conn_cnt) conn_cnt,ROUND(sum(conn_time_total)/sum(conn_cnt) ) avg_conn_time FROM ads_licode_conn_time_total
    WHERE dt='%s'
    and labtype='ec' and biztype in ('todesk','other')
    """ % (date)

def get_stat_by_region(date, customer_type):
    return """
   select 
	t1.*,cast(connection_count/total_connection_count*100 as DECIMAL(16,1)) connection_rate,7_total_connection_count,connection_count-7_total_connection_count as 7_day_diff
from
(
	SELECT * FROM ads_su_province_total
	WHERE dt='%s'
	AND customer_type='%s'
)t1
left join (
	select dt,sum(connection_count) total_connection_count  FROM ads_su_province_total
	WHERE dt='%s' AND customer_type='%s' group by dt
)t2
on t1.dt=t2.dt
left join
(
                SELECT province_name,connection_count 7_total_connection_count FROM ads_su_province_total
                WHERE dt=DATE_SUB('%s',INTERVAL 7 day)
                AND customer_type='%s'
)t3
on t1.province_name=t3.province_name
order by connection_count desc
    """ % (date, customer_type,date, customer_type,date, customer_type)

def get_stat_by_country(date, customer_type):
    return """
    select 
        t1.*,cast(connection_count/total_connection_count*100 as DECIMAL(16,2)) connection_rate
from
(
        SELECT * FROM ads_su_country_total
        WHERE dt='%s'
        AND customer_type='%s'
)t1
left join (
        select dt,sum(connection_count) total_connection_count  FROM ads_su_country_total
        WHERE dt='%s' AND customer_type='%s' group by dt
)t2
on t1.dt=t2.dt
order by connection_count desc
    """ % (date, customer_type,date, customer_type)

def get_stat_by_dcountry(date, customer_type):
    return """
    select 
        t1.*,cast(connection_count/total_connection_count*100 as DECIMAL(16,2)) connection_rate
from
(
        SELECT * FROM ads_du_country_total
        WHERE dt='%s'
        AND customer_type='%s'
)t1
left join (
        select dt,sum(connection_count) total_connection_count  FROM ads_du_country_total
        WHERE dt='%s' AND customer_type='%s' group by dt
)t2
on t1.dt=t2.dt
order by connection_count desc
    """ % (date, customer_type,date, customer_type)
    
def get_region_handshake_stat(date):
    return """
    SELECT 
        dt,
        serv,
        handshake_success_rate_now,
        handshake_success_rate_7day,
        diff
    FROM ads_serv_diff_total
    WHERE dt='%s'
    """ % (date)

def get_bad_serv_top(date):
    return """
    select 
	serv,
	handshake_success_count,
	authentication_success_count,
	new_connect_count,
	handshake_success_rate,
	authentication_success_rate,cast(new_connect_count/total_connection_count*100 as DECIMAL(16,1)) connection_rate
from
(
	SELECT
	dt,
        serv,
        handshake_success_count,
        authentication_success_count,
        new_connect_count,
        handshake_success_rate,
        authentication_success_rate
  FROM ads_bad_serv_top
	WHERE dt='%s'
)t1
left join (
	select dt,sum(new_connect_count) total_connection_count  FROM ads_bad_serv_top
	WHERE dt='%s'  group by dt
)t2
on t1.dt=t2.dt
order by handshake_success_rate 
    """ % (date,date)

def get_region_handshake_receover_stat(date):
    return """
    SELECT
        serv,
        handshake_success_rate_2day,
        handshake_success_rate_1day,
        diff,
        handshake_success_rate_8day,
        diff1
    from ads_serv_recover_total
    where dt='%s'""" % date

def get_ads_forward_hour_isp_total(date):
    return """
        select
            client,isp,hour,handshake_success_count,connection_count,handshake_success_rate,handshake_success_rate1,handshake_success_rate1-handshake_success_rate as  diff
        from ads_forward_hour_isp_total
        where dt='%s' order by client,isp desc,hour
        """ % date

def get_ads_ec_hour_isp_total(date):
    return """
	select
	    client,isp,hour,handshake_success_count,connection_count,handshake_success_rate,handshake_success_rate1,handshake_success_rate1-handshake_success_rate as  diff
	from ads_ec_hour_isp_total
	where dt='%s' order by client,isp desc,hour
        """ % date

def get_ads_ea_hour_isp_total(date):
    return """
        select
            hour,handshake_success_count,connection_count,handshake_success_rate,handshake_success_rate1,handshake_success_rate1-handshake_success_rate as  diff
        from ads_ea_hour_isp_total
        where dt='%s' order by hour
        """ % date

def get_ads_cross_provincial_total(date):
    return """
    SELECT 
        if(is_relay='1','是','否') is_relay,
        handshake_success_count,
        connection_success_count,
        connection_count,
	handshake_success_rate,
        session_rate
    FROM ads_cross_provincial_total
    WHERE dt='%s'
    """ % (date)

# In[144]:


tables = {
    "ads_session_day" : {
        "handshake_success_count" : "握手成功数",
        "connection_success_count" : "鉴权成功数",
        "connection_count" : "发起连接数",
        "handshake_success_rate" : "握手成功率",
        "session_rate" : "鉴权成功率"
    },
    "ads_session_day_user" : {
	"enterprise" : "用户",
        "handshake_success_count" : "握手成功数",
        "connection_success_count" : "鉴权成功数",
        "connection_count" : "发起连接数",
        "handshake_success_rate" : "握手成功率",
        "session_rate" : "鉴权成功率"
    },
    "ads_session_day_reconnect" : {
        "reconnect" : "是否重连",
        "handshake_success_count" : "握手成功数",
        "connection_success_count" : "鉴权成功数",
        "connection_count" : "发起连接数",
        "handshake_success_rate" : "握手成功率",
        "session_rate" : "鉴权成功率"
    },
    "ads_cross_provincial_total" : {
        "is_relay" : "是否跨省",
        "handshake_success_count" : "握手成功数",
        "connection_success_count" : "鉴权成功数",
        "connection_count" : "发起连接数",
        "handshake_success_rate" : "握手成功率",
        "session_rate" : "鉴权成功率"
    },
    "ads_ec_total" : {
        "succcess_count" : "连接成功数",
        "connection_count" : "发起连接数",
        "succcess_rate": "成功率"
    },
    "ads_ea_total" : {
        "succcess_count" : "连接成功数",
        "connection_count" : "发起连接数",
        "succcess_rate": "成功率"
    },
    "ads_conn_duration" : {
        "duration_total" : "连接总时长",
        "session_count": "会话成功数",
        "duration_average": "会话平均时长"
    },
    "ads_licode_conn_time_total": {
        "conn_time_total": "总连接时长",
        "conn_cnt": "连接次数",
        "avg_conn_time": "平均连接时长"
    },
    "ads_su_province_total" : {
        "province_name": "省份",
        "handshake_success_count": "握手成功数",
        "connection_success_count": "鉴权成功数",
        "connection_count": "发起连接数",
        "handshake_success_rate": "握手成功率",
        "session_rate": "鉴权成功率",
        "bit_rate": "码率",
        "connection_rate" : "占比",
	"7_total_connection_count" : "上周发起连接数",
	"7_day_diff" : "周增长连接数"
    },
    "ads_su_country_total" : {
        "country_name": "国家",
        "handshake_success_count": "握手成功数",
        "connection_success_count": "鉴权成功数",
        "connection_count": "发起连接数",
        "handshake_success_rate": "握手成功率",
        "session_rate": "鉴权成功率",
        "bit_rate": "码率",
        "connection_rate" : "占比"
    },
    "ads_du_country_total" : {
        "country_name": "国家",
        "handshake_success_count": "握手成功数",
        "connection_success_count": "鉴权成功数",
        "connection_count": "发起连接数",
        "handshake_success_rate": "握手成功率",
        "session_rate": "鉴权成功率",
        "bit_rate": "码率",
        "connection_rate" : "占比"
    },
    "ads_serv_diff_total" : {
        "serv": '机房',
        "handshake_success_rate_now": '当天握手成功率',
        "handshake_success_rate_7day": '一周前握手成功率',
        "diff": '下降百分点'
    },
    "ads_serv_recover_total" : {
        "serv": '机房',
        "handshake_success_rate_2day": '前天故障连通率',
        "handshake_success_rate_1day": '昨天连通率',
        "diff": '上升百分点',
        "handshake_success_rate_8day": '8天前连通率',
        "diff1": '期望上升百分点'
   },
   "ads_bad_serv_top" : {
	"serv": '机房',
	"handshake_success_count": '握手成功数',
	"authentication_success_count": '鉴权成功数',
	"new_connect_count": '发起连接数',
	"handshake_success_rate": '握手成功率',
	"authentication_success_rate": '鉴权成功率',
	"connection_rate" : "占比"
   },
   "ads_forward_hour_isp_total" : {
	"client": '控制方',
	"isp": '运营商',
	"hour": '小时',
	"handshake_success_count": '握手成功数',
	"connection_count": '发起连接数',
	"handshake_success_rate": '握手成功率',
	"handshake_success_rate1": '7天前握手成功率',
	"diff": '下降百分点'
   },
   "ads_ec_hour_isp_total" : {
	"client": '控制方',
        "isp": '运营商',
	"hour": '小时',
        "handshake_success_count": '握手成功数',
        "connection_count": '发起连接数',
        "handshake_success_rate": '握手成功率',
        "handshake_success_rate1": '7天前握手成功率',
        "diff": '下降百分点'
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
	    "forward连通率(总)" : get_forward_connection_success_rate_query(date),
	    "forward连通率(用户维度)" : get_forward_user_connection_success_rate_query(date),
	    "forward连通率(重连维度)" : get_forward_reconnect_connection_success_rate_query(date),
	    "forward连通率(跨省维度)" : get_ads_cross_provincial_total(date),
            "ec连通率" : get_ads_ec_total_query(date),
	    "ea 主控连通率" : get_ads_ea_total(date),
            "c端forward会话时长": get_forward_connection_time_c_query(date),
            "b端forward会话时长": get_forward_connection_time_b_query(date),
            "licode会话时长": get_licode_connection_time_query(date),
	    "握手成功率<90": get_bad_serv_top(date),
            "机房握手成功率(下降>3% & 发起连接>1000)": get_region_handshake_stat(date),
            "前日机房连通率恢复情况": get_region_handshake_receover_stat(date),
            "c端发起连接主控省份统计": get_stat_by_region(date, "customer"),
            "b端发起连接主控省份统计": get_stat_by_region(date, "business"),
            "c端发起连接主控国家统计top30": get_stat_by_country(date, "customer"),
            "c端连接被控国家统计top30": get_stat_by_dcountry(date, "customer"),
	    "b端发起连接主控国家统计top30": get_stat_by_country(date, "business"),
	    "b端连接被控国家统计top30": get_stat_by_dcountry(date, "business"),
            "forward分时运营商统计(同比下降1%)": get_ads_forward_hour_isp_total(date),
	    "ec分时运营商统计(同比下降1%)": get_ads_ec_hour_isp_total(date)
           }

def get_metrics():
    return {
            "forward连通率(总)" : tables["ads_session_day"],
            "forward连通率(用户维度)" : tables["ads_session_day_user"],
	    "forward连通率(重连维度)" : tables["ads_session_day_reconnect"],
	    "forward连通率(跨省维度)" : tables["ads_cross_provincial_total"],
            "ec连通率" : tables["ads_ec_total"],
	    "ea 主控连通率" : tables["ads_ea_total"],
            "c端forward会话时长": tables["ads_conn_duration"],
            "b端forward会话时长": tables["ads_conn_duration"],
            "licode会话时长": tables["ads_licode_conn_time_total"],
            "握手成功率<90": tables["ads_bad_serv_top"],
            "机房握手成功率(下降>3% & 发起连接>1000)": tables["ads_serv_diff_total"],
            "前日机房连通率恢复情况": tables["ads_serv_recover_total"],
            "c端发起连接主控省份统计": tables["ads_su_province_total"],
            "b端发起连接主控省份统计": tables["ads_su_province_total"],
            "c端发起连接主控国家统计top30": tables["ads_su_country_total"],
	    "c端连接被控国家统计top30": tables["ads_du_country_total"],
            "b端发起连接主控国家统计top30": tables["ads_su_country_total"],
            "b端连接被控国家统计top30": tables["ads_du_country_total"],
            "forward分时运营商统计(同比下降1%)": tables["ads_forward_hour_isp_total"],
            "ec分时运营商统计(同比下降1%)": tables["ads_ec_hour_isp_total"]
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
    recipients = ['backend@zuler.io', 'media_team@zuler.io', 'platform@zuler.io', 'zhumin@zuler.io','td_product@zuler.io']
    html_content = get_html(entry_to_content)
    send_email(email_sender, email_sender_pass, recipients, html_content, date)
