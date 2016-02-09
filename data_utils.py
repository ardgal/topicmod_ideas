__author__ = 'mcardle'
"""


"""
import PgContentSource as pgres
import requests
import bs4
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

SQL_INSERT = u"""INSERT INTO table_with_unstruc("ID", "TITLE", "DESCRIPTION", "ISSUE_DATE", "ISSUE_OFFICE", "URL")
VALUES (?,?,?,?,?);"""

SQL_SEL = u"SELECT \"ID\", \"URL\" FROM table_with_unstruc where LENGTH(\"DESCRIPTION\") <= 1;"

SQL_UPDATE= u"UPDATE table_with_unstruc SET \"DESCRIPTION\"=? WHERE \"ID\"=?;"


FILE_INS_SRC = ''
FILE_WEE_TEST = ''

BEGIN_CLAUSE = u'Dear ' # all main content is after the Dear Mr Smith:
END_CLAUSE = u'writing within fifteen (15) working days' #everything this after this is threats!

MAX_SIZE_DOC = 30000

"""

Load the data from the Excel txt file into the database

"""
def load_data():
    stopping = True

    db_connect = pgres.PgContentSource()

    with open(FILE_INS_SRC) as f, db_connect as connection:
        cursor = connection.cursor()
        lines = f.readline().split('\r')

        line_iter = iter(lines)
        next(line_iter)  # ignore titles

        for line in line_iter:
            _, issue_date, issue_office, subject, url, Id = line.split('\t')
            cursor.execute(SQL_INSERT, (Id,subject,'',issue_date, issue_office, url))
        cursor.close()

"""

extract the main part of the document we're interested in
and remove the HTML tags

"""
def extract_pretty_text(raw_doc):
    main_text = u''
    lines = raw_doc.split('\r\n')
    for line in lines:
        start_position = line.find(BEGIN_CLAUSE)
        if start_position >= 0: #bingo, we have the part where the real letter starts
            end_position = line.find(END_CLAUSE)
            main_text = line[start_position:end_position]
            break

    soup = bs4.BeautifulSoup(main_text,"html.parser")
    paragraphs = soup.find_all('p')
    last_line_index = len(paragraphs)-1
    document = ''
    for para in paragraphs[:last_line_index]:
        document += (para.get_text() + '\n')

    return document

"""

Main routine

"""
def update_description():
    db_connect = pgres.PgContentSource()
    with db_connect as connection:
        cursor = connection.cursor()
        cursor.execute(SQL_SEL)
        urls = cursor.fetchall()
        for record in urls:
            Id, url = record[0], record[1]
            raw_document = fetch_document(url)
            document = extract_pretty_text(raw_document)
            if len(document) >= MAX_SIZE_DOC:
                document = document[0:MAX_SIZE_DOC-1]
            cursor.execute(SQL_UPDATE, (document, Id))
            logging.info('Database update complete')
        cursor.close()


"""

Fetch the document from the URL given in the database.
Seemed to overwhelm server if we don't use random x seconds between retrieval!

"""
def fetch_document(url):
    # wait couple of seconds, avoid overwhelming server
    import time, random
    paws = random.randrange(5,15)
    logging.info('Taking a {0} second break '.format(paws))
    time.sleep(paws)
    logging.info('Requesting document from server')
    response = requests.get(url)
    return unicode(response.text)




#load_data()
#update_description()