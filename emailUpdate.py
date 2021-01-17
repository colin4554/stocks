import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os.path
import base64
from email.mime.text import MIMEText


# Define the SCOPES. If modifying it, delete the token.pickle file.
SCOPES = ['https://www.googleapis.com/auth/gmail.send']


def authorizeCreds():
    # Variable creds will store the user access token.
    # If no valid token found, we will create one.
    creds = None

    # The file token.pickle contains the user access token.
    # Check if it exists
    if os.path.exists('token.pickle'):
        # Read the token from the file and store it in the variable creds
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

            # If credentials are not available or are invalid, ask the user to log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

            # Save the access token in token.pickle file for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds


def create_message(subject, message_text):
  """Create a message for an email.

  Args:
    sender: Email address of the sender.
    to: Email address of the receiver.
    subject: The subject of the email message.
    message_text: The text of the email message.

  Returns:
    An object containing a base64url encoded email object.
  """
  message = MIMEText(message_text)
  message['to'] = 'colin.flueck@gmail.com'
  message['from'] = 'GCP Stock News Scraper Bot'
  message['subject'] = subject
  print(message)
  return {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}


def send_message(creds, message):
  """Send an email message.

  Args:
    service: Authorized Gmail API service instance.
    user_id: User's email address. The special value "me"
    can be used to indicate the authenticated user.
    message: Message to be sent.

  Returns:
    Sent Message.
  """
  service = build('gmail', 'v1', credentials=creds)

  try:
    message = (service.users().messages().send(userId='me', body=message)
               .execute())
    print('Message Id: %s' % message['id'])
    return message
  except Exception as e:
    print('An error occurred: %s' % e)

def sendEmail(subject, message_text):
    try:
        creds = authorizeCreds()
        email = create_message(subject, message_text)
        send_message(creds, email)
        return "email sent successfully"
    except Exception as e:
        return 'An error occurred: %s' % e

# subject ='Stock Test'
#
# message_text = "the following tickers were scrape..."
#
# print(sendEmail(subject, message_text))