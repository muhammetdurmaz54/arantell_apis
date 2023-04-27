# import pywhatkit
# import time
# now=time.localtime()
# pywhatkit.sendwhatmsg("+918652369630","hooo",now.tm_hour,now.tm_min+2)


import smtplib
from email.message import EmailMessage

def email_alert(subject,body,to):
  msg=EmailMessage()
  msg.set_content(body)
  msg['subject']=subject
  msg['to']=to

  user="dorodoro790@gmail.com"
  msg['from']=user
  password="owdlpmhjtoazzhpc"

  server=smtplib.SMTP("smtp.gmail.com",587)
  server.starttls()
  server.login(user,password)
  server.send_message(msg)
  server.quit()

email_alert("hii","aya aya aya", "dorodoro790@gmail.com")