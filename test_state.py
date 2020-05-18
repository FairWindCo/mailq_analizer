#!/usr/local/bin/python3.8

import subprocess, sys
from collections import Counter
from email.mime.text import MIMEText
from subprocess import Popen, PIPE


result = subprocess.run(['mailq'], stdout=subprocess.PIPE)
text =  result.stdout.decode('utf-8')

block = text.splitlines()

message = None
start_message = 0
senders = Counter()
messages = 0

if len(block) == 1:
    print("NO MESSAGE")
    sys.exit()

for index, line in enumerate(block):
    if index == 0:
        continue
    if line.startswith('--'):
        columns = line.split()
        messages = columns[4]
        if message is not None:
            print(message)
        print('RESULT MESSAGES COUNT {}:'.format(messages))
    if not line:
        continue
    if not line.startswith(' '):
        if message is None:
            message = {}
        else:
            print(message)
        start_message=index
        elements = line.split()
        message['id'] = elements[0]
        if len(elements) >= 7:
            senders.update(elements[6])
    else:
        if message is None:
            message = {}    
        line = line.strip()
        if line.startswith('('):
            message['error'] = line
            message['to'] = []
        else:
            if 'to' not in message: 
                message['to']=[]
            message['to'].append(line)


if int(messages) > 1000:
    msg = MIMEText("To  many Message in Postfix QUERY {}".format(messages))
    msg["From"] = "admin@tarelki.com.ua"
    msg["To"] = "sergey.manenok@gmail.com"
    msg["Subject"] = "SYS ALERT MESSAGE."
    p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
    p.communicate(msg.as_string().encode())