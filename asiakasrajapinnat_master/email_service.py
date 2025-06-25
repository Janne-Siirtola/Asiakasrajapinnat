import os
import logging

from azure.communication.email import EmailClient


def send_email(subject: str, plain_text: str, ) -> str:
    """Send an email and return the operation ID."""
    email_client = EmailClient.from_connection_string(
        os.getenv("COMMUNICATION_SERVICES_CONNECTION_STRING"))
    sender_address = os.getenv("SENDER_ADDRESS")
    logging.info(f"Using sender address: {sender_address}")
    message = {
        "content": {
            "subject": subject,
            "plainText": plain_text
        },
        "recipients": {
            "to": [
                {
                    "address": "janne.siirtola@remeo.fi",
                    "displayName": "Janne Siirtola (Work)"
                },
                {
                    "address": "janne.siirtola@outlook.com",
                    "displayName": "Janne Siirtola (Personal)"
                }
            ]
        },
        "senderAddress": sender_address
    }

    # Send and wait (poll) for the result
    poller = email_client.begin_send(message)
    result = poller.result()

    if result["status"] == "Succeeded":
        return result["id"]
    else:
        logging.exception(f"Failed to send email: {result.get('error')}")
        raise Exception(f"Failed to send email: {result.get('error')}")
