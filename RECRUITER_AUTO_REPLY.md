# Recruiter Auto-Reply Script

This script monitors your inbox for emails from recruiters and automatically sends replies with your CV attached.

## Features

- **Automatic Recruiter Detection**: Uses keyword analysis to identify recruiting-related emails
- **Duplicate Prevention**: Tracks processed emails in a SQLite database to avoid duplicate replies
- **CV Attachment**: Automatically attaches your CV (Eessam_Azzam_Profile.docx) to replies
- **Continuous Monitoring**: Can run continuously to check emails at regular intervals
- **Microsoft Graph Integration**: Uses existing MSAL configuration for secure email access

## Prerequisites

1. **CV File**: Make sure `Eessam_Azzam_Profile.docx` is in the same directory as the script
2. **Environment Variables**: Uses existing MSAL configuration from your `.env` file:
   - `TENANT_ID`
   - `CLIENT_ID`
   - `CLIENT_SECRET`
   - `SENDER_UPN`
   - `TOKEN_CACHE_FILE`

## Usage

### One-time Check (Last 24 Hours)

```bash
python recruiter_auto_reply.py
```

This will:
- Check your inbox for unread emails from the past 24 hours
- Identify recruiter emails based on keywords
- Send automatic replies with your CV
- Track processed emails to prevent duplicates

### Continuous Monitoring

Edit `recruiter_auto_reply.py` and uncomment the last line:

```python
# responder.run_continuous(check_interval_minutes=5)
```

Then run:

```bash
python recruiter_auto_reply.py
```

The script will check for new recruiter emails every 5 minutes (configurable).

### With Custom Time Range

Modify the `check_incoming_emails()` call to specify hours:

```python
count = responder.check_incoming_emails(hours_back=12)  # Check last 12 hours
```

## How It Works

1. **Email Retrieval**: Fetches unread emails from your inbox
2. **Recruiter Detection**: Analyzes subject and body for recruiter keywords:
   - Searches for terms like "recruiter", "hiring", "position", "opportunity", etc.
   - Requires multiple keyword matches for high confidence
3. **Reply Generation**: 
   - Creates a professional response email
   - Attaches your CV file in base64 format
   - Maintains email thread using Microsoft Graph reply endpoint
4. **Tracking**: Stores processed email IDs in `recruiter_responses.sqlite` to prevent duplicate replies

## Tracked Data

The script creates a `recruiter_responses.sqlite` database with:
- Email ID (to prevent reprocessing)
- Sender email and name
- Email subject
- Received and processed timestamps

## Customization

You can customize:

### Reply Message Template
Edit the email body in the `_send_reply_with_cv()` method:

```python
email_body = f"""
Hi {sender_name},

[Your custom message here]
"""
```

### Recruiter Keywords
Modify the `RECRUITER_KEYWORDS` list at the top of the script to add/remove detection terms.

### Check Interval
Change the sleep interval in `run_continuous()`:

```python
time.sleep(check_interval_minutes * 60)  # Default: 5 minutes
```

## Permissions Required

The script requires the following Microsoft Graph permissions (delegated):
- `Mail.Read` - Read emails
- `Mail.Send` - Send emails

## Troubleshooting

- **CV file not found**: Ensure `Eessam_Azzam_Profile.docx` is in the same directory
- **Token errors**: Check that your `.env` file has valid MSAL credentials
- **No emails detected**: Check your inbox for unread emails; the script only processes unread messages
- **Duplicate prevention not working**: The database tracks by message ID; clear `recruiter_responses.sqlite` to reprocess

## Safety Notes

- Review the email template before enabling continuous mode
- The script only replies to recruiter emails it's confident about
- All replies are saved to your Sent Items for review
- You can always manually disable replies by stopping the script
