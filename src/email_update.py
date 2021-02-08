"""Send emails from a google account with a given subject and message

Notes:
    - TODO: set the EMAIL_ADDRESS variable (recipient email address) below before using
    - the first time the program runs, you will need to complete authorization with your google account
"""

import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os.path
import base64
from email.mime.text import MIMEText


# set this before running!
EMAIL_ADDRESS = ""

# this scope allows us to send emails
SCOPES = ['https://www.googleapis.com/auth/gmail.send']


def authorizeCreds():
    """Ensures user is authorized on this account

    Returns:
      Credentials
    """
    creds = None

    if os.path.exists('../token.pickle'):
        # read the token from the file and store as creds
        with open('../token.pickle', 'rb') as token:
            creds = pickle.load(token)

            # If credentials are not available or are invalid, ask the user to log in through browser
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('../credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

            # saves creds so sign in isn't necessary next time
        with open('../token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds


def create_message(subject, message_text):
  """Create a message for an email

  Args:
    subject: The subject of the email message
    message_text: The text of the email message

  Returns:
    An object containing a base64url encoded email object
  """
  message = MIMEText(message_text)
  message['to'] = EMAIL_ADDRESS
  message['from'] = 'GCP Stock News Scraper Bot'
  message['subject'] = subject
  return {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}


def send_message(creds, message):
  """Send an email message

  Args:
    creds: Authorized Gmail API credentials
    message: Message to be sent

  Returns:
    Nothing, but prints if message id if successful or error message if not
  """
  service = build('gmail', 'v1', credentials=creds)

  try:
    message = (service.users().messages().send(userId='me', body=message)
               .execute())
    print('Message Id: %s' % message['id'])
  except Exception as e:
    print('An error occurred: %s' % e)

def send_email(subject, message_text):
    """Compiles message, completes authorization, and sends email

    Args:
      subject: The subject of the email message
      message_text: The text of the email message

    Returns:
      Success or error message
    """
    try:
        creds = authorizeCreds()
        email = create_message(subject, message_text)
        send_message(creds, email)
        return "Email Message sent successfully"
    except Exception as e:
        return 'An error occurred: %s' % e


# Allows this module to be run by itself, with user inputted subject and message
if __name__ == "__main__":
    subject = input('Enter the email\'s subject: ')

    message_text = input('Enter the email\'s message: ')

    print(send_email(subject, message_text))
