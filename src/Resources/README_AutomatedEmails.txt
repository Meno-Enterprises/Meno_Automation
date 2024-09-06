This is a script that sends automated emails. It is intended to be called by other scripts when needed. 
It uses the smtplib library to send emails via an SMTP server. The email configuration is loaded from a JSON file, which should contain the SMTP server details, sender and recipient addresses, and other relevant information.

The send_email method takes the path to the email configuration file, the subject and body of the email, and an optional list of file paths for attachments. 
It constructs the email message, attaches any files, and sends the email using the specified SMTP server.

If using a business gmail account, less secure apps needs to be turned on or the script needs to be reconfigured for another login method.
Read more at https://support.google.com/accounts/answer/6010255?authuser=2&hl=en&authuser=2&visit_id=638612498785653222-2486335120&p=less-secure-apps&rd=1

send_email method parameters:

    email_config_path (str): The path to the email configuration JSON file. Required. If the file is not found or vital details are missing, the method will return without sending an email.
    subject (str): The subject of the email. Optional, defaults to an empty string.
    body (str): The body of the email. Optional, defaults to an empty string.
    file_attachment_paths (list of str): A list of file paths for attachments. Optional, defaults to None.

Example configuration file (email_config.json):

{
    "smtp_server": "smtp.example.com",
    "smtp_port": 587,
    "smtp_username": "username@user.com",
    "smtp_password": "password123",
    "from_name": "Sender Name",
    "from_email": "sender@example.com",
    "to_email": ["recipient@example.com"],
    "cc_email": ["cc_recipient1@example.com", "cc_recipient2@example.com"],
    "bcc_email": []
}