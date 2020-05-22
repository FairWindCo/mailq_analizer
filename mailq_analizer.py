#!/usr/local/bin/python3.8
import io
import subprocess
from collections import Counter
from email.mime.text import MIMEText
from subprocess import Popen, PIPE

MAX_MAIL_PER_USER_TO_BLOCK = 1500
MAX_MAIL_IN_QUERY_TO_CLEAN = 1500
MAX_MAIL_PER_USER_TO_WARN = 150


def get_mailq_data():
    result = subprocess.run(['mailq'], stdout=subprocess.PIPE)
    text = result.stdout.decode('utf-8')
    block = text.splitlines()
    return block


def get_mailq():
    message = None
    senders = Counter()
    messages = 0
    index = 0

    with subprocess.Popen(['mailq'], stdout=subprocess.PIPE) as proc:
        for line in io.TextIOWrapper(proc.stdout, encoding="utf-8"):
            index, messages, senders, message = line_analise(line, index, messages, senders, message)
            if messages > 0:
                break

    analise_mail_counters(senders, messages)
    return int(messages), senders


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


def line_analise(line, index=0, messages=0, senders=None, message=None, print_result=False):
    if senders == None:
        senders = Counter()
    if index == 0:
        return 1, messages, senders, message

    if line.startswith('--'):
        # END MAILQ COMMAND OUTPUT
        columns = line.split()
        messages = int(columns[4])
        if message is not None:
            print_result and print(message)
        print('RESULT MESSAGES COUNT {}:'.format(messages))
    elif not line or line == '\n':
        if message is not None:
            print_result and print(message)
            senders.update([message['from']])
    elif not line.startswith(' '):
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
    return index + 1, messages, senders, message


def analize_mailq(block, print_result=True):
    message = None
    senders = Counter()
    messages = 0

    if len(block) == 1:
        print("NO MESSAGE")
        return 0, senders

    for index, line in enumerate(block):
        i, messages, senders, message = line_analise(line, index, messages, senders, message, print_result)
        if messages > 0:
            break
    analise_mail_counters(senders, messages)
    return int(messages), senders


def analize_mailq_buffered(block):
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
    analise_mail_counters(senders, messages)
    return int(messages), senders


def form_mail_report_message(messages, counters, form_total_email=True):
    data_list = ['{}:{}\n'.format(k, v) for k, v in counters.items()]
    total = ['']
    if form_total_email:
        total = ['{}:{}\n'.format(k, v) for k, v in read_email()]
    return ''.join(["To  many Message in Postfix QUERY {}\n".format(messages), *data_list, *total])


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
    if messages >= MAX_MAIL_PER_USER_TO_WARN:
        print(form_mail_report_message(messages, counters))
    data = load_data_from_disk('disk.log.txt')
    alarm, disk_states = analize_disk(data)
    if alarm:
        print(form_disk_report_message(disk_states))


def form_log_metrics(filename='last_action.log'):
    from datetime import datetime
    datetime_ = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
    with open(filename, "w") as f:
        print(datetime_)
        f.write(datetime_)


def block_user(user_name, block=True):
    print(f'Try block user: {user_name}')
    try:
        block = 1 if block else 0
        result = subprocess.run(['mysql', 'mail'],
                                input=f"update users set disabled={block} where email='{user_name}'\n".encode())
        if result.returncode == 0:
            print(f'User: {user_name} - locked')
    except Exception as e:
        print(e)


def read_email():
    try:
        result = subprocess.run(['mysql', 'mail'], stdout=subprocess.PIPE,
                                input=f"select email, disabled from users\n".encode())
        if result.returncode == 0:
            lines = [line.split() for line in result.stdout.decode('utf-8').splitlines()]
            return lines
        else:
            return []
    except Exception as e:
        print(e)
        return []


def clear_postfix_mailq():
    try:
        result = subprocess.run(['postsuper', '-d', 'ALL'])
        if result.returncode == 0:
            print(f'QUERY CLEARED')
    except Exception as e:
        print(e)


def analise_mail_counters(counters, messages):
    for mail, count in counters.items():
        if count > MAX_MAIL_PER_USER_TO_BLOCK:
            if mail.find('@tarelki.com.ua'):
                block_user(mail)
    if messages > MAX_MAIL_IN_QUERY_TO_CLEAN:
        clear_postfix_mailq()


def main():
    # data = get_mailq_data()
    # messages, counters = analize_mailq(data)
    messages, counters = get_mailq()
    if messages >= MAX_MAIL_PER_USER_TO_WARN:
        send_mail(form_mail_report_message(messages, counters))
    data = get_df_data()
    alarm, disk_states = analize_disk(data)
    if alarm:
        send_mail(form_disk_report_message(disk_states))


if __name__ == '__main__':
    form_log_metrics()
    import os

    if os.name == 'nt':
        test()
    else:
        main()
