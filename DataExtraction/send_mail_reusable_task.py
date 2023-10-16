#!/usr/bin/python
import os.path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
import logging
from decouple import Config, RepositoryEnv


def send_mail(to, cc, subject, body):
    present_working_directory = os.getcwd()
    env_file = os.path.join(present_working_directory, 'ENV', 'env.env')
    env_file = Config(RepositoryEnv(env_file))
    logging.info("Sending Notification mail")
    smtp_username = env_file('SMTP_USER_NAME')

    smtp_password = env_file('SMTP_PASSWORD')

    smtp_host = env_file("SMTP_HOST")

    smtp_port = env_file('SMTP_PORT')

    body_text = body

    sender = env_file('DEFAULT_SENDER_EMAIL')

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = to
    msg['Cc'] = cc

    part1 = MIMEText(body_text, 'plain')

    msg.attach(part1)

    try:
        smtp = smtplib.SMTP(smtp_host, smtp_port)
        smtp.ehlo()
        smtp.starttls()
        smtp.login(smtp_username, smtp_password)
        logging.debug("SMTP connection is established")
        smtp.ehlo()
        smtp.sendmail(sender, to, str(msg))
        print("sent mail")
        logging.info("Notification mail has been sent")
    except smtplib.SMTPException as smtp_exception:
        print("Error: unable to send email")
        logging.error("Exception occurred while sending mail notification")
        logging.exception(smtp_exception)
        print(str(smtp_exception))


def send_mail_with_attachment(to, cc, subject, body, attachment_path=None):
    present_working_directory = os.getcwd()
    env_file = os.path.join(present_working_directory, 'ENV', 'env.env')
    env_file = Config(RepositoryEnv(env_file))
    logging.info("Sending Notification mail")
    sender = env_file('DEFAULT_SENDER_EMAIL')
    smtp_username = env_file('SMTP_USER_NAME')

    smtp_password = env_file('SMTP_PASSWORD')

    smtp_host = env_file("SMTP_HOST")

    smtp_port = env_file('SMTP_PORT')

    body_text = body

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = to
    msg['Cc'] = cc

    if attachment_path is not None:
        logging.info("Attachment is valid file")
        if os.path.isfile(attachment_path):
            print("Attachment {} file path provided is a valid file".format(attachment_path))
            logging.debug("Attachment {} file path provided is a valid file".format(attachment_path))
        else:
            logging.debug("Attachment {} file path provided is not a valid file".format(attachment_path))
            raise Exception("Attachment file path provided is not a valid file")
        file_part = MIMEBase('application', "octet-stream")
        with open(attachment_path, 'rb') as file:
            file_part.set_payload(file.read())
        encoders.encode_base64(file_part)
        file_part.add_header('Content-Disposition',
                             'attachment; filename={}'.format(Path(attachment_path).name))
        msg.attach(file_part)
        logging.debug("attachment is added to the mail compose")
    else:
        print("no attachment is found to send the mail")
        logging.debug("no attachment is found to send the mail")

    part1 = MIMEText(body_text, 'plain')

    msg.attach(part1)

    try:
        smtp = smtplib.SMTP(smtp_host, smtp_port)
        smtp.ehlo()
        smtp.starttls()
        smtp.login(smtp_username, smtp_password)
        logging.debug("SMTP connection is established")
        smtp.ehlo()
        smtp.sendmail(sender, to, str(msg))
        print("sent mail")
        logging.info("Notification mail has been sent")
    except smtplib.SMTPException as smtp_exception:
        print("Error: unable to send email")
        logging.error("Exception occurred while sending mail notification")
        logging.exception(smtp_exception)
        print(str(smtp_exception))
        raise smtp_exception


if __name__ == '__main__':
    pass