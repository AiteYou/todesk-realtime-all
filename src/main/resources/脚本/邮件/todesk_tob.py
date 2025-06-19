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
        msg['To'] = ', '.join(recipients)  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        msg['Subject'] = "todesk_B端每日报表: %s" % report_date  # 邮件的主题，也可以说是标题
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

def get_ads_to_b_connect_query(date):
    return """
    SELECT company_id,cnt,link_time
    FROM ads_tob_company_total
    WHERE dt='%s' and flag=1 order by cnt desc
    """ % (date)

def get_ads_to_b_sconnect_city_query(date):
    return """
    SELECT company_id,cnt,flag_list
    FROM ads_tob_company_total
    WHERE dt='%s' and flag=4 order by cnt desc
    """ % (date)

def get_ads_to_b_dconnect_city_query(date):
    return """
    SELECT company_id,cnt,flag_list
    FROM ads_tob_company_total
    WHERE dt='%s' and flag=5 order by cnt desc
    """ % (date)

def get_ads_to_b_city_query(date):
    return """
    SELECT company_id,cnt,flag_list
    FROM ads_tob_company_total
    WHERE dt='%s' and flag=2 order by cnt desc
    """ % (date)

def get_ads_to_b_ip_query(date):
    return """
    SELECT company_id,cnt,flag_list
    FROM ads_tob_company_total
    WHERE dt='%s' and flag=3 order by cnt desc
    """ % (date)



tables = {
    "ads_tob_company_total" : {
        "company_id" : "公司id",
        "cnt" : "连接数",
	"link_time" : "连接时长(s)"
    },
    "ads_tob_company_sconnect_city_total" : {
        "company_id" : "公司id",
        "cnt" : "数量",
        "flag_list" : "城市"
    },
    "ads_tob_company_dconnect_city_total" : {
        "company_id" : "公司id",
        "cnt" : "数量",
        "flag_list" : "城市"
    },
    "ads_tob_company_city_total" : {
        "company_id" : "公司id",
        "cnt" : "数量",
	"flag_list" : "城市"
    },
    "ads_tob_company_IP_total" : {
        "company_id" : "IP",
        "cnt" : "数量",
	"flag_list" : "公司ID"
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
    return {"发起连接数(>=100)" : get_ads_to_b_connect_query(date),
	    "连接主控城市(>=5)" : get_ads_to_b_sconnect_city_query(date),
	    "连接被控城市(>=5)" : get_ads_to_b_dconnect_city_query(date),
	    "公司登录城市数(>=5)" : get_ads_to_b_city_query(date),
            "IP登录公司数(>=2)" : get_ads_to_b_ip_query(date)
           }

def get_metrics():
    return {
            "发起连接数(>=100)" : tables["ads_tob_company_total"],
	    "连接主控城市(>=5)" : tables["ads_tob_company_sconnect_city_total"],
	    "连接被控城市(>=5)" : tables["ads_tob_company_dconnect_city_total"],
	    "公司登录城市数(>=5)" : tables["ads_tob_company_city_total"],
            "IP登录公司数(>=2)" : tables["ads_tob_company_IP_total"]
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
    recipients = ['liuchao@zuler.io', 'liuyinghui@zuler.io', 'guoji@zuler.io', 'dongguangwei@zuler.io']
    html_content = get_html(entry_to_content)
    send_email(email_sender, email_sender_pass, recipients, html_content, date)


