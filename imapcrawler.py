# -*- coding: utf-8 -*-
"""
IMAP Email Inbox Crawler
Optional dependencies for improved extraction quality can be installed with pip:
  pip install tqdm beautifulsoup4 python-dateutil mail-parser-reply chromadb

This tool allows you to crawl emails from an IMAP server, 
fetch raw email data, and clean/parse the content for later analysis and usage for e.G a knowledge base.
"""

import imaplib
import email
from email.header import decode_header
import os
import getpass
from datetime import timedelta, datetime, timezone
import random
import time
import warnings
# import dateparser
from dateutil import parser
import json
from pathlib import Path
import copy, json

import re

try:
    from tqdm import tqdm    
except ImportError as err:
    class tqdm:
        def __init__(self, iterable=None, *args, **kwargs):
            self.inp = iterable or []
            self.iterable = iter(self.inp)
            self.i = 0
        def __len__(self):
            return getattr(self.iterable, '__len__', 0)

        def __iter__(self):
            return self
        
        def __next__(self):
            p = round(self.i/len(self)) if len(self) > 0 else 'NaN'
            print(f'{self.i}/{len(self)} {p}%')
            self.i +=1
            return next(self.iterable)

try:
    from mailparser_reply import EmailReplyParser    
except ImportError as err:
    print("No mailparser_reply installed... skipping this")


try:
    from bs4 import BeautifulSoup    
except ImportError as err:
    print('No BeautifulSoup installed. Falling back to native python for HML parsing')


# %pip install tqdm beautifulsoup4 mail-parser-reply




DESCRIPTION = __doc__
# Precompile regex patterns for better performance
SPLITTER_PATTERNS = [
    re.compile(re.escape(s), re.IGNORECASE) for s in [
        'Mit freundlichen Grüßen',
        'Viele Grüße',
        'Best Regards',
        'Mit freundlichen Gr??en',
        '***********',
        '\n--\n',
        '\n---\n',
        '\nOHB Digital Connect GmbH\n',
        r'^>.*\n^>.*',
        r'^-----Urspr*\n',
        r'^\s*from\s.*\s*wrote:\s*$',
    ]
]

# import questionary

# Configuration file path
CONFIG_FILE = str(Path(os.path.expanduser('~/.imapcrawler_config.json')).resolve())
CHOICES_MODE = ['merge', 'overwrite', 'raise']

DEFAULT_CONFIG = {
        'server': '',
        'email': '',
        'mode': 'merge',
        'filepath_raw': 'emails_raw.jsonl',
        'filepath_clean': 'emails.jsonl',
        'filepath_vecdb': 'email_vec_db', 
        'collection_name': 'emails'
    }
    
def load_config():
    """Load configuration from file or return default config"""
    new_config = {k:'' for k in DEFAULT_CONFIG} # empty will be asked interactively
    
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                new_config.update({k:config.get(k) for k in new_config if k in config})
                return new_config
    except (json.JSONDecodeError, IOError):
        pass
    
    return new_config

def save_config(config):
    """Save configuration to file"""
    try:
        os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
    except IOError:
        print(f"Warning: Could not save configuration to {CONFIG_FILE}")


def write_records_jsonlines(path, recs):
    # Save records to JSONL file
    with open(path, 'w') as f:
        for r in recs:
            f.write(json.dumps(r) + '\n')

def read_records_jsonlines(path):
    with open(path, 'r') as f:
        return [json.loads(r.strip()) for r in f.readlines()]
                
def connect_imap(server, username, password):
    """Connect to IMAP server"""
    mail = imaplib.IMAP4_SSL(server)
    mail.login(username, password)
    return mail


def fetch_msg_uid(mail, email_id):
    try:
        status, msg_data = mail.fetch(email_id, '(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID DATE FROM)])')
        raw_header = msg_data[0][1]
        if status != 'OK':
            return None
        # Parse the header
        msg = email.message_from_bytes(raw_header)

        uid = lambda r: ' | '.join((str(r[k]) for k in 'message_id date_iso from'.split()))
        cleaned_str = msg['Date'].split(', ')[-1].split(' (')[0]  # Only between ', ' and '('
        dt = parser.parse(cleaned_str).astimezone(timezone.utc)
        dc = {
            'date_iso': dt.isoformat().replace('+00:00', 'Z'),
            'message_id': msg.get('Message-ID', '').strip(),
            'from': msg['From'].replace('"', '').replace('\r', '').replace('\n', ' '),
        }

        return uid(dc)    
    except Exception as err:
        print(err)
        return None
    


def fetch_emails(mail, folder='INBOX', limit=100, search_criteria = 'ALL', is_inner=False, to_skip_uids=None):
    """Fetch emails from folder"""
    # Properly quote folder names with spaces or special characters
    if ' ' in folder or '(' in folder or ')' in folder:
        folder_quoted = f'"{folder}"'
    else:
        folder_quoted = folder
        
    # Select the folder properly
    status = mail.select(folder)
    if status[0] != 'OK':
        # Try without quoting if that fails
        status = mail.select(folder_quoted)
        if status[0] != 'OK':
            return []
    
    status, messages = mail.search(None, search_criteria)
    
    # Get list of email IDs
    email_ids = messages[0].split()
    if limit and limit > 0:
        email_ids = email_ids[-limit:]  # Get last N emails
    
    to_skip_uids = set() if not to_skip_uids else set(to_skip_uids)
    result = []
    for email_id in tqdm(email_ids, desc=f"Emails {folder.ljust(40)}", leave=is_inner):
        msg_data = None
        # Fetch email
        msg_uid = fetch_msg_uid(mail, email_id) if to_skip_uids else None
        if not msg_uid or not (msg_uid in to_skip_uids):
            status, msg_data = mail.fetch(email_id, '(RFC822)')
        else:
            fetch_emails.n_skipped += 1
            fetch_emails.uids_skipped.append(msg_uid)

        if not msg_data is None:
            msg = email.message_from_bytes(msg_data[0][1])
            result.append((email_id.decode(), msg))

    return result

fetch_emails.n_skipped = 0
fetch_emails.uids_skipped = []

def query_emails_all_folders(mail, limit=100, search_criteria="ALL", is_inner=False, to_skip_uids=None):
    """
    Query emails from all folders for a specific month
    
    Args:
        mail: IMAP connection object
        target_month: Month in format "YYYY-MM" (e.g., "2026-03")
    
    Returns:
        Dictionary mapping folder names to lists of (email_id, message) tuples
    """
    mail.select("INBOX")
    
    
    # Get all folders
    status, folders = mail.list()
    if status != 'OK':
        raise Exception("Failed to retrieve folders")
    
    folders = [f.decode().split(' "." ')[-1].strip().strip('"') for f in folders]
    folder_list = [f for f in folders if f.startswith('INBOX') or "sent" in f.lower() or f.lower().startswith('Archives')]

    # Create result structure
    results = []
    # Search for emails in each folder
    
    for folder in tqdm(folder_list, desc=f"Folders", leave=is_inner):
        # print(folder)
        try:
            a = fetch_emails(mail, folder, limit=limit, search_criteria=search_criteria, is_inner=True, to_skip_uids=to_skip_uids)
            results += a
            if limit > 0:
                limit -= len(a)
                if limit <= 0:
                    break
                
        except Exception as e:
            warnings.warn(f"Error processing folder {folder}: {e} ... will skip")
            continue
    
    return results

def query_emails_between(mail, start_date: datetime, end_date:datetime, limit=100, to_skip_uids=None):
    """
    Query emails from all folders for a specific month
    
    Args:
        mail: IMAP connection object
    
    Returns:
        Dictionary mapping folder names to lists of (email_id, message) tuples
    """
    if isinstance(start_date, str):
        start_date = parser.parse(start_date).astimezone(timezone.utc)
    
    if isinstance(end_date, str):
        end_date = parser.parse(start_date).astimezone(timezone.utc)

    # Format date search string
    start_str = start_date.strftime("%d-%b-%Y")
    end_str = end_date.strftime("%d-%b-%Y")
    search_criteria = f'(SINCE {start_str} BEFORE {end_str})'

    results = query_emails_all_folders(mail, limit, search_criteria, to_skip_uids=to_skip_uids)
    return results


def query_emails_month(mail, target_month: str, limit=100, to_skip_uids=None):
    """
    Query emails from all folders for a specific month
    
    Args:
        mail: IMAP connection object
        target_month: Month in format "YYYY-MM" (e.g., "2026-03")
    
    Returns:
        Dictionary mapping folder names to lists of (email_id, message) tuples
    """
    # Parse the target month
    try:
        target_date = datetime.strptime(target_month, "%Y-%m")
    except ValueError:
        raise ValueError("Invalid month format. Use YYYY-MM format (e.g., 2026-03)")
    
    # Define the date range for the target month
    start_date = target_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if target_date.month == 12:
        end_date = target_date.replace(year=target_date.year + 1, month=1, day=1)
    else:
        end_date = target_date.replace(month=target_date.month + 1, day=1)

    return query_emails_between(mail, start_date, end_date, limit, to_skip_uids=to_skip_uids)


def query_emails_day(mail, target_day: str, limit=100, to_skip_uids=None):
    """
    Query emails from all folders for a specific month
    
    Args:
        mail: IMAP connection object
        target_day: Month in format "YYYY-MM-DD" (e.g., "2026-03")
    
    Returns:
        Dictionary mapping folder names to lists of (email_id, message) tuples
    """
    # Parse the target month
    try:
        target_date = datetime.strptime(target_day, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Invalid month format. Use YYYY-MM format (e.g., 2026-03)")
    
    # Define the date range for the target month
    start_date = target_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if target_date.month == 12:
        end_date = target_date.replace(year=target_date.year + 1, month=1, day=1)
    else:
        end_date = target_date.replace(month=target_date.month + 1, day=1)

    return query_emails_between(mail, start_date, end_date, limit, to_skip_uids=to_skip_uids)




if 'BeautifulSoup' in globals():
        
    def html_to_text(email_content):
        """
        Convert HTML email content to plain text if it starts with '<!DOCTYPE html>'
        
        Args:
            email_content (str): The email content to process
            
        Returns:
            str: Plain text version if HTML detected, otherwise original content
        """

        soup = BeautifulSoup(email_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text and clean it up
        text = soup.get_text()
        
        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text
else:
    def html_to_text(email_content):

        # # Check if content looks like HTML
        # if not email_content.strip().lower().startswith('<!doctype html') and \
        # not email_content.strip().lower().startswith('<html'):
        #     return email_content
        
        # Use html module to unescape HTML entities and remove tags
        import html
        import re
        
        # Remove HTML tags
        clean_text = re.sub(r'<[^>]+>', '', email_content)
        
        # Unescape HTML entities
        clean_text = html.unescape(clean_text)
        
        # Remove extra whitespace
        clean_text = re.sub(r'\s+', ' ', clean_text)
        
        # Strip leading/trailing whitespace
        clean_text = clean_text.strip()
        
        return clean_text

def extract_email(email_id, msg):
        #     # Extract information
    try:
        subject = decode_header(msg['Subject'])[0][0]    
    except Exception as err:
        # print(msg)
        print(msg['Subject'])
        return {}
    
    if isinstance(subject, bytes):
        subject = subject.decode('utf-8', errors='replace').replace('�', '?')
    
    subject = subject.replace('\r', '').replace('\n', ' ')
    
    ignores = {'multipart/', 'application/', 'text/calendar', 'image/' }
    # Get body
    body_parts = []
    def parse_part(part):
        mime = part.get_content_type()
        if mime == "text/plain" or mime == "text/html":
            v = part.get_payload(decode=True).decode('utf-8', errors='replace').replace('�', '?')
            return [mime, v]
        elif [mime.startswith(i) for i in ignores]:
            return [mime, '']
        else:
            print(f'unknown content type: {mime=}')
            return [mime, '']
        
    if msg.is_multipart():
        body_parts = [parse_part(part) for part in msg.walk()]
    else:
        body_parts = [parse_part(msg)]

    body = [[mime, val] for (mime, val) in body_parts if val]
    if not body:
        return {}
    
    # body = html_to_text(body)
    
    
    in_reply_to = msg.get('In-Reply-To')
    references = msg.get('References')
    
    # Extract thread ID from References or In-Reply-To
    thread_id = ''
    if references:
        # References contains the full thread chain
        thread_id = references.strip().split()[-1]  # Get the last message ID in chain

    # Get all recipient headers
    to_recipients = msg.get('To', '')
    cc_recipients = msg.get('Cc', '')
    bcc_recipients = msg.get('Bcc', '')  # Note: Bcc is often not included in fetched emails

    cleaned_str = msg['Date'].split(', ')[-1].split(' (')[0]  # Only between ', ' and '('
    dt = parser.parse(cleaned_str).astimezone(timezone.utc)
    iso_string = dt.isoformat().replace('+00:00', 'Z')

    

    dc = {
        'email_id': str(email_id),
        'message_id': msg.get('Message-ID'),
        
        'from': msg['From'].replace('"', '').replace('\r', '').replace('\n', ' '),
        
        'date_iso': iso_string,
        'thread_id': thread_id,

        'subject': subject,
        'body': body,
        
        'in_reply_to': in_reply_to.strip() if in_reply_to else '',
        'references': references,

        'to_list': to_recipients, #[addr.strip() for addr in to_recipients.split(',') if addr.strip()],
        'cc_list': cc_recipients, # [addr.strip() for addr in cc_recipients.split(',') if addr.strip()],
        'bcc_list': bcc_recipients, # [addr.strip() for addr in bcc_recipients.split(',') if addr.strip()],

        'date': msg['Date'],
        
        # 'attachments': attachments,        
    }

    uid = lambda r: ' | '.join((str(r[k]) for k in 'message_id date_iso from'.split()))
    dc['uid'] = uid(dc)
    return dc

def get_mail(inp):
    m = [extract_email(k,v) for (k, v) in inp]
    return [mm for mm in m if mm]

def remove_angle_bracket_content(text):
    # Optimized regex pattern to match content between < and >
    pattern = r'<[^>]*>'
    # Replace matched content with empty string
    result = re.sub(pattern, '', text)
    return result


# Efficient attachment filtering
def is_valid_attachment(attachment):
    filename = attachment['filename']
    content_type = attachment['content_type']
    return not (
        (not '.' in filename and content_type.startswith('image')) or 
        filename.endswith('.ics')
    )

# def itersplit(body):
#     lines = body.split('\n')
#     res = []
#     quote_count = 0
    
#     for line in lines:
#         stripped_line = line.strip()
        
#         # Check for quote lines
#         if stripped_line.startswith('>'):
#             quote_count += 1
#         else:
#             quote_count = 0
            
#         # Handle quote removal logic
#         if quote_count > 2 or (
#             stripped_line.lower().startswith('from') and 
#             stripped_line.lower().endswith('wrote:')
#         ):
#             # Remove last two lines if we hit the condition
#             if len(res) >= 2:
#                 res = res[:-2]
#             break
            
#         res.append(line)
    
#     body = '\n'.join(res)
#     return body


def clean_record(record):
    body_parts = record.get("body", ["text/plain", "NO_EMAIL_BODY"])
    
    body = ''
    for mime, val in body_parts:
        v = html_to_text(val) if 'html' in mime else val
        body += v + '\n\n'

    
    # Early exit for short bodies
    if len(body) < 50:
        return {}
    
    # Remove angle bracket content
    body = remove_angle_bracket_content(body)
    
    # Parse email reply
    if 'EmailReplyParser' in globals():
        body = EmailReplyParser(languages=['en', 'de']).parse_reply(text=body)
    
    # Split by multiple splitters efficiently
    for pattern in SPLITTER_PATTERNS:
        body = pattern.split(body)[0]
    
    # # Process lines with optimized logic
    # body = itersplit(body)
    
    # Final length check
    if len(body) < 50:
        return {}
    
    if body.strip().startswith('Updated invitation: '):
        return {}
    
    # Update the record
    cleaned_record = copy.deepcopy(record)

    cleaned_record["content"] = body.strip()
    cleaned_record.pop('body')

    # Optimize list to string conversion
    for k in ['to_list', 'cc_list', 'bcc_list']:
        if isinstance(cleaned_record[k], list):
            cleaned_record[k] = ','.join(cleaned_record[k])
    
    
    # attachments = [str(a['filename']) for a in cleaned_record['attachments'] if is_valid_attachment(a)]
    # cleaned_record['attachments'] = ', '.join(attachments)
    
    return cleaned_record


def main_get_raw(email, password=None, date=None, month=None, limit=-1, mode='merge', filepath_raw='emails_raw.jsonl', diff=False, **kwargs):

    if password is None:
        password = getpass.getpass('Enter Email password')

    print('connecting...')
    # Connect and fetch emails
    mail = connect_imap('imap.mpifr-bonn.mpg.de', email, password)
    
    print('getting emails...')
    if date is None and month is None:
        input(f'WARNING! Given was {date=} {month=} this will result in querying ALL data... this can potentially take very long (hours) please press enter to confirm (ctrl+c to abort)')
    
    if diff:
        print('diff is enabled. will load old data and diff...')
        print(f'reading from {filepath_raw=}')
        recs = read_records_jsonlines(filepath_raw)
        to_skip_uids = set([r['uid'] for r in recs])
        print(f'found N={len(to_skip_uids)} to_skip_uids')
    else:
        to_skip_uids = None
        recs = None

    t0 = time.time()
    if date is None and month is None:
        emails = query_emails_all_folders(mail, limit=limit, to_skip_uids=to_skip_uids)
    elif date is None:
        emails = query_emails_month(mail, month, limit=limit, to_skip_uids=to_skip_uids)
    elif month is None:
        emails = query_emails_day(mail, date, limit=limit, to_skip_uids=to_skip_uids)
    else:
        raise ValueError('invalid input. Need to give either date or month, not both')
    
    # Add info about skipped emails
    if fetch_emails.n_skipped > 0:
        print(f"Skipped {fetch_emails.n_skipped} emails")
        # if fetch_emails.uids_skipped:
        #     print(f"Skipped UIDs: {fetch_emails.uids_skipped}")
    else:
        print(f"Did not skip any emails")

    

    t1 = time.time()
    print(f'getting emails...DONE got N={len(emails)} in {t1-t0:.1f}sec')

    
    print('unpacking emails...')
    t0 = time.time()
    records = get_mail(emails)
    records = [r for r in records if not 'Adobe Acrobat Sign' in r['from']]
    records.sort(key=lambda r: r['date_iso'])
    t1 = time.time()
    print(f'unpacking emails...DONE got N={len(records)} in {t1-t0:.1f}sec')

    if to_skip_uids:
        print(f'adding previously skipped entries to record set from old data (N={len(records)} before)...')
        records += [r for r in recs if r['uid'] in fetch_emails.uids_skipped]
        print(f'adding previously skipped entries to record set from old data (N={len(records)} after)...DONE')

    # outpath = f'C:/Users/tglaubach/Nextcloud/documents/email_archive/emails_raw.jsonl'
    if filepath_raw:
        if mode == 'merge' and os.path.exists(filepath_raw):
            print('merging old and new emails...')
            print(f'reading from {filepath_raw=}')
            recs = read_records_jsonlines(filepath_raw)
            print(f'got N={len(recs)} old raw emails.')

            recs = {r['uid']:r for r in recs}
            recs.update({r['uid']:r for r in records})
            records = list(recs.values())  
            print('merging old and new raw emails... DONE')
        elif mode == 'raise' and os.path.exists(filepath_raw):
            raise FileExistsError(f'{filepath_raw=} already exists and {mode=}. Set mode to either "merge" or "overwrite"')
        elif mode=='overwrite' and os.path.exists(filepath_raw):
            print(f'overwriting existing file at {filepath_raw=} because {mode=}')
        elif mode=='overwrite' and os.path.exists(filepath_raw):
            print(f'creating new file at {filepath_raw=}')

        print('saving raw emails...')
        t0 = time.time()
        print(f'writing N={len(records)} records to: {filepath_raw=}')
        write_records_jsonlines(filepath_raw, records)
        t1 = time.time()
        print(f'saving raw emails... DONE in {t1-t0:.1f}sec')

    return records


def main_get_clean(mode='merge', filepath_clean='emails.jsonl', filepath_raw='emails_raw.jsonl', limit=-1, **kwargs):
    
    print(f'reading from {filepath_raw=}')
    records = read_records_jsonlines(filepath_raw)
    print(f'got N={len(records)} emails from filepath_raw')

    if limit and limit > 0 and limit < len(records):
        records = records[:limit]

    print('cleaning raw emails...')
    t0 = time.time()
    records = [clean_record(r) for r in tqdm(records)]
    records = [r for r in records if r]
    t1 = time.time()
    print(f'cleaning raw emails...DONE got N={len(records)} in {t1-t0:.1f}sec')


    if filepath_clean:
        if mode == 'merge' and os.path.exists(filepath_clean):
            print('merging old and new emails...')
            print(f'reading from {filepath_clean=}')
            recs = read_records_jsonlines(filepath_clean)
            print(f'got N={len(recs)} old emails.')

            recs = {r['uid']:r for r in recs}
            recs.update({r['uid']:r for r in records})
            records = list(recs.values())  
            print('merging old and new emails... DONE')
        elif mode == 'raise' and os.path.exists(filepath_clean):
            raise FileExistsError(f'{filepath_clean=} already exists and {mode=}. Set mode to either "merge" or "overwrite"')
        elif mode=='overwrite' and os.path.exists(filepath_clean):
            print(f'overwriting existing file at {filepath_clean=} because {mode=}')
        elif mode=='overwrite' and os.path.exists(filepath_clean):
            print(f'creating new file at {filepath_clean=}')

        print('saving cleaned emails...')
        t0 = time.time()
        print(f'writing N={len(records)} records to: {filepath_clean=}')
        write_records_jsonlines(filepath_clean, records)
        t1 = time.time()
        print(f'saving cleaned emails... DONE in {t1-t0:.1f}sec')

    return records


# def set_config_interactive(config_old, config_new=None):
    
#     cnfg = copy.deepcopy(config_old)
#     for key in config_old:
#         kwargs = {}
#         if key.startswith('filepath'):
#             prompt = questionary.path 
#         elif key == 'mode':
#             prompt = questionary.select
#             kwargs = dict(choices = CHOICES_MODE)
#         else:
#             prompt = questionary.text

#         value = config_new.get(key, config_old.get(key, ''))
#         if not value:
#             value = prompt(f'default value for "{key}"?', DEFAULT_CONFIG.get(key), **kwargs).ask()
        
#         if not value:
#             value = ''

#         cnfg[key] = value

def main_vecdb(filepath_clean:str, filepath_vecdb:str, collection_name:str, limit:int=-1, mode='merge', *args, **kwargs):
    import chromadb

    pth = str(filepath_clean)
    with open(pth, 'r') as fp:
        if pth.endswith('.jsonl'):
            records = [json.loads(s) for s in fp.readlines()]
        elif pth.endswith('.json'):
            records = json.loads(fp.read())
        else:
            raise ValueError(f'Unknown file format for {pth=}')

    records = list(records)
    records = list({r['uid']:r for r in records}.values())

    if limit > 0 and len(records) > limit:
        records = records[:limit]

    if mode == 'raise' and os.path.exists(filepath_vecdb):
        raise FileExistsError(f"file with {filepath_vecdb=} already exists and 'mode' is {mode=}")
    
    client = chromadb.PersistentClient(path=filepath_vecdb)
    collection = client.get_or_create_collection(name=collection_name)

    # Get all existing IDs before adding new data
    existing_ids = []
    
    if mode == 'merge':
        try:
            results = collection.get(include=[])
            existing_ids = results['ids']
        except Exception as e:
            print(f"Error retrieving existing IDs: {e}")
            raise


    existing_ids = set(existing_ids)
    if mode == 'merge':
        records = [r for r in records if not r['uid'] in existing_ids]

    if not records:
        print('Nothing to upload...')
    elif len(records) > 20:
        for r in tqdm(records, 'records'):
            collection.add(
                documents=[r['content']],
                metadatas=[{k: str(v) for k, v in r.items() if k != 'body'}],
                ids=[r['uid']],
            )
    else:
        collection.add(
            documents=[r['content'] for r in records],
            metadatas=[{k: str(v) for k, v in r.items() if k != 'body'} for r in records],
            ids=[r['uid'] for r in records],
        )


def main_vecdb_query(query:str, filepath_vecdb:str, collection_name:str, limit:int=10, *args, **kwargs):
    import chromadb


    client = chromadb.PersistentClient(path=filepath_vecdb)
    collection = client.get_or_create_collection(name=collection_name)

    n_results = max(int(limit), 1)

    results = collection.query(
        query_texts=[query],
        n_results=n_results,
        # where={"metadata_field": "is_equal_to_this"}, # optional filter
        # where_document={"$contains":"search_string"}  # optional filter
    )
    
    n = len(results['documents'][0])
    records = [{'distance': results['distances'][0][i], 'body': results['documents'][0][i], **results['metadatas'][0][i]} for i in range(n)]

    print(f'got {len(records)=} for {query=}')
    # print(json.dumps(results, indent=2))

    return records
        

        
    
def test_config(config):
    empty_values = {k:v for k, v in config.items() if v == '' or v is None}
    if empty_values:
        raise ValueError(f'found {empty_values=} need to either set them permanently, or give them as command line args')

def _peek(records, show_body, all=False):
    if all:
        ii = range(len(records))
        print(f'Found N={len(records)=} randomly selected index ALL')
    else:
        ii = [random.randrange(0, len(records), 1)]
        print(f'Found N={len(records)=} randomly selected index {ii=}')
    print('\n\n' + '§'*100)
    for i in ii:
        if show_body:
            print(records[i]['subject'])
            print('')
            print(records[i].get('body', records[i]['content']))
        else:
            print(json.dumps(records[i], indent=2))
    print('\n\n' + '§'*100)

def main():
    import argparse

    
    

    config = load_config()


    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    
    # Create subparsers for different actions
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    cnfg_parser = subparsers.add_parser('config-set', help='Update the persistent config interactively')
    cnfg_parser.add_argument('--server', help='IMAP server to connect to', default=None)
    cnfg_parser.add_argument('--email', help='Email address to connect with', default=None)
    cnfg_parser.add_argument('--mode', choices= CHOICES_MODE, default=None, help='File handling mode')
    cnfg_parser.add_argument('-r', '--filepath_raw', default=None, help='Output file path')
    cnfg_parser.add_argument('-c', '--filepath_clean', default=None, help='Output file path')
    cnfg_parser.add_argument('-d', '--filepath_vecdb', default=None, help='Output file path vecdb')
    cnfg_parser.add_argument('--collection_name', default=None, help='collection_name for vecdb')
    
    

    cnfg_printer = subparsers.add_parser('config-show', help='show the current config')
    cnfg_printer.add_argument('--dummy', action='store_true', help='dummy argument placeholder')

    cnfg_clearer = subparsers.add_parser('config-clear', help='clear the current config to all empty')
    cnfg_clearer.add_argument('--dummy', action='store_true', help='dummy argument placeholder')

    cnfg_default = subparsers.add_parser('config-default', help='set the current config to factory defaults')
    cnfg_default.add_argument('--dummy', action='store_true', help='dummy argument placeholder')
    
    # Raw email fetching parser
    raw_parser = subparsers.add_parser('download', help='Fetch raw emails from IMAP server')
    raw_parser.add_argument('--diff', help='whether or not to skip already known emails', action='store_true')
    raw_parser.add_argument('--email', help='Email address to connect with')
    raw_parser.add_argument('--password', help='Email password (will prompt if not provided)')
    raw_parser.add_argument('--date', help='Specific date to query (YYYY-MM-DD)')
    raw_parser.add_argument('--month', help='Month to query (YYYY-MM)')
    raw_parser.add_argument('--limit', type=int, default=-1, help='Limit number of emails to fetch (-1 for all)')
    raw_parser.add_argument('--server', help='IMAP server to connect to', default=config['server'])
    raw_parser.add_argument('--mode', choices=CHOICES_MODE, default=config['mode'], help='File handling mode')
    raw_parser.add_argument('-r', '--filepath_raw', default=config['filepath_raw'], help='Output file path')
    raw_parser.add_argument('-c', '--filepath_clean', default=config['filepath_clean'], help='Output file path for clean emails')
    raw_parser.add_argument('-p', '--peek', help='whether or not to peek a rendom result after done', action='store_true')
    raw_parser.add_argument('-b', '--body', help='whether or not to only peek subject and body', action='store_true')

    # Clean email parser
    clean_parser = subparsers.add_parser('clean', help='Clean raw emails and save to file')
    clean_parser.add_argument('--mode', choices=CHOICES_MODE, default=config['mode'], help='File handling mode')
    clean_parser.add_argument('-r', '--filepath_raw', default=config['filepath_raw'], help='Output file path for raw emails')
    clean_parser.add_argument('-c', '--filepath_clean', default=config['filepath_clean'], help='Output file path for clean emails')
    clean_parser.add_argument('-p', '--peek', help='whether or not to peek a rendom result after done', action='store_true')
    clean_parser.add_argument('-b', '--body', help='whether or not to only peek subject and body', action='store_true')
    clean_parser.add_argument('--limit', type=int, default=-1, help='Limit number of emails to fetch (-1 for all)')

    peek_raw = subparsers.add_parser('peek-raw', help='show a random email from raw file')
    peek_raw.add_argument('-r', '--filepath_raw', default=config['filepath_raw'], help='Output file path')
    peek_raw.add_argument('-b', '--body', help='whether or not to only show subject and body', action='store_true')

    peek_clean = subparsers.add_parser('peek-clean', help='show a random email from clean file')
    peek_clean.add_argument('-r', '--filepath_clean', default=config['filepath_clean'], help='Output file path')
    peek_clean.add_argument('-b', '--body', help='whether or not to only show subject and body', action='store_true')

    vec_parser = subparsers.add_parser('vec-make', help='upload clean records to vector_db (using chroma)')
    vec_parser.add_argument('--mode', choices=CHOICES_MODE, default=config['mode'], help='File handling mode')
    vec_parser.add_argument('-c', '--filepath_clean', default=config['filepath_clean'], help='Input file path for clean emails')
    vec_parser.add_argument('-d', '--filepath_vecdb', default=config['filepath_vecdb'], help='Output file path for vectordb')
    vec_parser.add_argument('--limit', type=int, default=-1, help='Limit number of emails to fetch (-1 for all)')
    vec_parser.add_argument('--collection_name', default=config['collection_name'], help='collection_name for vecdb')
    
    vec_query = subparsers.add_parser('vec-query', help='query teh vector_db for a search string (using chroma)')
    vec_query.add_argument('-q', '--query', required=True, help='The Query')
    vec_query.add_argument('-d', '--filepath_vecdb', default=config['filepath_vecdb'], help='Output file path for vectordb')
    vec_query.add_argument('--limit', type=int, default=10, help='Limit number of emails to fetch')
    vec_query.add_argument('--collection_name', default=config['collection_name'], help='collection_name for vecdb')
    vec_query.add_argument('-b', '--body', help='whether or not to only show subject and body', action='store_true')
    args = parser.parse_args()    
    d = vars(args)

    # Execute appropriate function
    if args.command == 'download':
        config.update({k:v for k, v in d.items() if v and k != "command"})
        
        if not config.get('password', ''):
            config['password'] = getpass.getpass(f'Enter Email password for {config["email"]} @ {config["server"]}: ')

        test_config(config)
        records = main_get_raw(**config)
        if args.peek:
            print('='*20)
            _peek(records, args.body)
    elif args.command == 'clean':
        config.update({k:v for k, v in d.items() if v and k != "command"})
        test_config(config)
        records = main_get_clean(**config)
        if args.peek:
            print('='*20)
            _peek(records, args.body)
    elif args.command == 'vec-make':
        config.update({k:v for k, v in d.items() if v and k != "command"})
        test_config(config)
        records = main_vecdb(**config)
    elif args.command == 'vec-query':
        config.update({k:v for k, v in d.items() if v and k != "command"})
        records = main_vecdb_query(**config)
        print('='*20)
        _peek(records, args.body, all=True)
        
    elif args.command == 'config-show':
        print('='*20)
        print(f'Content for {CONFIG_FILE=} (file_exists={os.path.exists(CONFIG_FILE)})')
        
        print(json.dumps(config, indent=2))
        print('='*20)
    elif args.command == 'config-set':
        config.update({k:v for k, v in d.items() if v and k != "command"})
        print(f'saving new config to {CONFIG_FILE=}')
        save_config(config)
    elif args.command == 'config-clear':
        print(f'saving empty config to {CONFIG_FILE=}')
        config = {k:'' for k in config.keys()}
        save_config(config)
    elif args.command == 'config-default':
        print(f'saving default config to {CONFIG_FILE=}')
        save_config(DEFAULT_CONFIG)
    elif args.command == 'peek-raw':
        config.update({k:v for k, v in d.items() if v and k != "command"})
        test_config(config)
        filepath_raw = config["filepath_raw"]
        print('='*20)
        print(f'Content for {filepath_raw=} (file_exists={os.path.exists(CONFIG_FILE)})')
        records = read_records_jsonlines(filepath_raw)
        _peek(records, args.body)
        print('='*20)
    elif args.command == 'peek-clean':
        config.update({k:v for k, v in d.items() if v and k != "command"})
        test_config(config)
        filepath_clean = config["filepath_clean"]
        print('='*20)
        print(f'Content for {filepath_clean=} (file_exists={os.path.exists(CONFIG_FILE)})')
        records = read_records_jsonlines(filepath_clean)
        _peek(records, args.body)
        print('='*20)
    else:
        parser.print_help()

if __name__ == '__main__':

    
    main()