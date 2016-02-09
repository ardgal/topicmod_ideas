__author__ = 'Andrew J McArdle'
"""

Gensim corpora sourced from database records.

See: https://radimrehurek.com/gensim/index.html

"""
from gensim import corpora, parsing, summarization
from bs4 import BeautifulSoup



class DbSQL(object):
    """
    Wrapper class to hold SQL SELECT statement and the position of the unique key, title and document
    in the result set (default position is 0,1,2 respectively)

    """

    def __init__(self, sql_stmnt, pk_loc=0, title_loc=1, doc_loc=2):
        self.pk_location = pk_loc
        self.title_loc = title_loc
        self.doc_loc = doc_loc
        self._sql = sql_stmnt

    @property
    def sql(self):
        return self._sql

    @sql.setter
    def sql(self, value):
        self._sql = value

    @property
    def unique_key_location(self):
        return self.pk_location

    @property
    def title_location(self):
        return self.title_loc

    @property
    def doc_location(self):
        return self.doc_loc



class DbSummary(object):
    def __init__(self, dbSQL, db_object, source_is_html=False):
        self.__dbSQL = dbSQL
        self.__db_object = db_object
        self.__record_identifiers = []
        if source_is_html:
            self.preprocessor = HtmlTagsRemover()
        else:
            self.preprocessor = None

    def __iter__(self):
        with self.__db_object as conn:
            cursor = conn.cursor()
            cursor.execute(self.__dbSQL.sql)
            record = cursor.fetchone()
            while record:
                document_text = record[self.__dbSQL.doc_location]
                pk, title = record[self.__dbSQL.unique_key_location], record[self.__dbSQL.title_location]

                if self.preprocessor:
                    title = self.preprocessor.clean_text(title)
                    document_text = self.preprocessor.clean_text(document_text)

                self.record_identifiers.append((pk, title))
                yield summarization.summarize(document_text)
                record = cursor.fetchone()

    @property
    def record_identifiers(self):
        """
        Get the 'unique keys' (primary key?) and titles
        :return: List of (pk, title) tuples
        :rtype  List
        """
        return self.__record_identifiers

#end Class DbSummary


class DbDictSource(object):
    """
        Class for creating the gensim corpora.dictionary object from a database connection.
        Not much use in itself. Create child class and override tokenize if you wat to do more than merely split
        sentences on spaces.  For example if you want to use stemmer or clean up the text do this in the tokenize
        method.

        :param  dbSQL   The object to source the SQL and the location of the unique key, title and 'document' fields
                        in the SQL recordset
        :type   DbSQL

        :param  db_object   The database object to provide the connection to the database
        :type   DbmsCore.JdbcConnection TODO: Change this to an interface

        :param  source_is_html    If the document is in html will trigger removal of html tags
        :type   Boolean (default = False)

    """
    def __init__(self, dbSQL, db_object, source_is_html=False):
        self.__dbSQL = dbSQL
        self.__db_object = db_object
        self.record = None
        if source_is_html:
            self.preprocessor = HtmlTagsRemover()
        else:
            self.preprocessor = None

    def __iter__(self):
        with self.db_object as conn:
            cursor = conn.cursor()
            cursor.execute(self.__dbSQL.sql)
            record = cursor.fetchone()
            while record:
                document_text = record[self.__dbSQL.doc_location]
                if self.preprocessor:
                    document_text = self.preprocessor.clean_text(document_text)

                yield parsing.preprocess_string(document_text)
                record = cursor.fetchone()

    @property
    def db_object(self):
        return self.__db_object

    @property
    def db_sql(self):
        return self.__dbSQL




class DbCorpus(object):
    """
        Gensim corpora sourced from a database connection.
        Uses the DbDictSource internally to create the corpora.dictionary.  The unique key and title fields
        can be retrieved from the record_identifiers property to allow clients to retrieve the database record
        at a point in the future.

        Builds the gensim corpora object using the dictionary_source object which sources documents from
        a database connection

        :param  dictionary_source   The database driven dictionary object from where we source words
        :type   DbDictSource

        :param  source_is_html    If the document is in html will trigger removal of html tags
        :type   Boolean (default = False)

    """
    def __init__(self, dictionary_source, source_is_html=False):
        self._length = 0
        self.dict_source = dictionary_source
        if source_is_html:
            self.preprocessor = HtmlTagsRemover()
        else:
            self.preprocessor = None
        self._dictionary = corpora.Dictionary(self.dict_source)
        once_ids = [token_id for token_id, doc_freq in self.dictionary.dfs.iteritems() if doc_freq == 1]
        self._dictionary.filter_tokens(once_ids)
        self._dictionary.compactify()
        self.__record_identifiers = []

    def __iter__(self):
        db = self.dict_source.db_object
        dbsql = self.dict_source.db_sql
        with db as conn:
            cursor = conn.cursor()
            cursor.execute(dbsql.sql)
            record = cursor.fetchone()
            while record:
                self._length += 1
                document_text = record[dbsql.doc_location]
                pk, title = record[dbsql.unique_key_location], record[dbsql.title_location]

                if self.preprocessor:
                    title = self.preprocessor.clean_text(title)
                    document_text = self.preprocessor.clean_text(document_text)

                self.record_identifiers.append((pk, title))
                tokens = parsing.preprocess_string(document_text)
                yield self._dictionary.doc2bow(tokens)
                record = cursor.fetchone()

    def __len__(self):
        return self._length

    @property
    def record_identifiers(self):
        """
        Get the 'unique keys' (primary key?) and titles
        :return: List of (pk, title) tuples
        :rtype  List
        """
        return self.__record_identifiers

    @property
    def dictionary(self):
        return self._dictionary




class HtmlTagsRemover(object):
    """

    Uses Beautiful Soup to remove HTML - for use if data contains rich text (common on modern systems)

    """
    def clean_text(self, text):
        soup = BeautifulSoup(text, 'html.parser')
        return soup.get_text()


