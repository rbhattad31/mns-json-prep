import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


def send_email(config_dict,subject, body, to_emails, attachment_path=None):
    try:
        # Email configuration
        sender_email = config_dict['sender_email']
        sender_password = config_dict['sender_password']
        smtp_server = config_dict['smtp_server']
        smtp_port = config_dict['smtp_port']
        to_email_string = ', '.join(to_emails)
        # Create the MIME object
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = to_email_string
        msg['Subject'] = subject

        # Attach body text
        msg.attach(MIMEText(body, 'plain'))

        # Attach file if specified
        if attachment_path:
            attachment = open(attachment_path, 'rb')
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= " + attachment_path)
            msg.attach(part)

        # Connect to the SMTP server
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, to_emails, msg.as_string())

        print("Email sent successfully!")

    except smtplib.SMTPAuthenticationError:
        print("Authentication error. Check your email and password.")
    except smtplib.SMTPException as e:
        print(f"SMTP error: {e}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    # Example usage
    from Config import create_main_config_dictionary
    path = r"C:\MCA Portal\Config.xlsx"
    sheet = 'Sheet1'
    json_file_path = r"C:\MCA Portal\U25200DL2014PTC263572\U25200DL2014PTC263572.json"
    cin = 'U25200DL2014PTC263572'
    config_dict, config_status = create_main_config_dictionary(path, sheet)
    cin_complete_subject = str(config_dict['cin_Completed_subject']).format(cin)
    cin_completed_body = str(config_dict['cin_Completed_body']).format(cin)
    emails = config_dict['to_email']
    emails = str(emails).split(',')
    send_email(config_dict,cin_complete_subject,cin_completed_body,emails,json_file_path)
