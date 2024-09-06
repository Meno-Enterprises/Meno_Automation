import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
import json

class AutomatedEmails:
    def __init__(self):
        pass

    def send_email(self, email_config_path, subject = "", body = "", file_attachment_paths = None): # string, string = "", string = "", list of strings = None
        def load_email_config(email_config_file_name):
            print(f"Loading email configuration from {email_config_file_name}...")
            try:
                with open(email_config_file_name, 'r') as file:
                    email_config = json.load(file)
                return email_config
            except FileNotFoundError:
                print(f"Email configuration file '{email_config_file_name}' not found.")
                return None

        # Load email configuration from JSON file
        email_config = load_email_config(email_config_path)
        if email_config is None: return

        # Set email configuration variables
        print("Setting email configuration variables...")
        smtp_server = email_config['smtp_server'] # string
        smtp_port = email_config['smtp_port'] # int
        smtp_username = email_config['smtp_username'] # string
        smtp_password = email_config['smtp_password'] # string
        from_name = email_config['from_name'] # string
        from_email = email_config['from_email'] # string
        to_email = email_config['to_email'] # list of strings
        cc_email = email_config['cc_email'] # list of strings
        bcc_email = email_config['bcc_email'] # list of strings

        # Create the email
        print("Generating email...")
        msg = MIMEMultipart()
        if from_email: msg['From'] = f"{from_name} <{from_email}>"
        else:
            print(f"No sender address in {email_config_path}.")
            return
        if to_email: msg['To'] = ', '.join(to_email)
        else:
            print(f"No recipient address in {email_config_path}.")
            return
        if cc_email: msg['Cc'] = ', '.join(cc_email)
        if bcc_email: msg['Bcc'] = ', '.join(bcc_email)
        msg['Subject'] = subject if subject else ""

        # Attach the body with the msg instance
        body = body if body else ""
        msg.attach(MIMEText(body, 'plain'))

        # Attach files if any
        if file_attachment_paths:
            for file_path in file_attachment_paths:
                try:
                    print(f"Attaching file '{file_path}'...")
                    with open(file_path, 'rb') as attachment:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(attachment.read())
                        encoders.encode_base64(part)
                        part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(file_path)}')
                        msg.attach(part)
                except FileNotFoundError:
                    print(f"File '{file_path}' not found. Aborting email.")
                    return
                except Exception as e:
                    print(f"Failed to attach file '{file_path}': {e}")
                    return

        # Combine all recipients
        all_recipients = to_email
        if cc_email: all_recipients += cc_email
        if bcc_email: all_recipients += bcc_email

        # Send the email
        try:
            print("Sending email...")
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
            server.login(smtp_username, smtp_password)
            text = msg.as_string()
            server.sendmail(from_email, all_recipients, text)
            server.quit()
            print("Email sent successfully.")
        except Exception as e:
            print(f"Failed to send email: {e}")

