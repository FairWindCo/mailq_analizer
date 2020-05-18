#!/usr/local/bin/python3.8

import subprocess
from collections import Counter
from email.mime.text import MIMEText
from subprocess import Popen, PIPE


def get_mailq_data():
    result = subprocess.run(['mailq'], stdout=subprocess.PIPE)
    text = result.stdout.decode('utf-8')
    block = text.splitlines()
    return block


def get_df_data():
    result = subprocess.run(['df', '-h'], stdout=subprocess.PIPE)
    text = result.stdout.decode('utf-8')
    block = text.splitlines()
    return block


def load_data_from_disk(filename):
    with open(filename) as f:
        lines = f.readlines()
        return lines


def send_mail(message, to='sergey.manenok@gmail.com', frm='admin@tarelki.com.ua'):
    msg = MIMEText(message)
    msg["From"] = frm
    msg["To"] = to
    msg["Subject"] = "SYS ALERT MESSAGE."
    p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
    p.communicate(msg.as_string().encode())


def analize_mailq(block):
    message = None
    senders = Counter()
    messages = 0

    if len(block) == 1:
        print("NO MESSAGE")
        return 0, senders

    for index, line in enumerate(block):
        if index == 0:
            continue

        if line.startswith('--'):
            # END MAILQ COMMAND OUTPUT
            columns = line.split()
            messages = columns[4]
            if message is not None:
                print(message)
            print('RESULT MESSAGES COUNT {}:'.format(messages))
            break

        if not line or line == '\n':
            if message is not None:
                print(message)
                senders.update([message['from']])
            continue

        if not line.startswith(' '):
            # MAILQ_ID, SENDER OR ERROR MESSAGE
            line = line.strip()

            if line.startswith('('):
                message['error'] = line
            else:
                elements = line.split()
                if len(elements) > 0:
                    sender = elements[6]
                    message = {'id': elements[0], 'from': sender, 'to': [], 'error': ''}
        else:
            # RECIPIETNS BLOCK
            line = line.strip()
            message['to'].append(line)

    return int(messages), senders


def form_mail_report_message(messages, counters):
    data_list = ['{}:{}\n'.format(k, v) for k, v in counters.items()]
    return ''.join(["To  many Message in Postfix QUERY {}\n".format(messages), *data_list])


def form_disk_report_message(disks_data):
    data_list = [f'DISK:{k} use:{v["percent"]}% - left:{v["left"]} mounted on {v["mount"]} \n' for k, v in
                 disks_data.items()]
    return ''.join(['NO MORE SPACE LEFT\n', *data_list])


def analize_disk(data):
    disk_state = {}
    alarm = False
    for item in data[1:]:
        elements = item.split()
        disk = elements[0]
        left = elements[3]
        percent = int(elements[4][:-1])
        mount = elements[5]
        if (percent > 99):
            alarm = True
        disk_state[disk] = {'percent': percent, 'left': left, 'mount': mount}
    return alarm, disk_state


def test():
    data = load_data_from_disk('log_mail.txt')
    messages, counters = analize_mailq(data)
    if messages >= 1000:
        print(form_mail_report_message(messages, counters))
    data = load_data_from_disk('disk.log.txt')
    alarm, disk_states = analize_disk(data)
    if alarm:
        print(form_disk_report_message(disk_states))


def main():
    data = get_mailq_data()
    messages, counters = analize_mailq(data)
    if messages >= 1000:
        send_mail(form_mail_report_message(messages, counters))
    data = get_df_data()
    alarm, disk_states = analize_disk(data)
    if alarm:
        send_mail(form_disk_report_message(disk_states))


if __name__ == '__main__':
    #test()
    main()
