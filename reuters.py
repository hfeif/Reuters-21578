import re
import glob
from bs4 import BeautifulSoup
import pandas as pd


def get_reuters_data():
    """
    Process the raw reuters 21578 dataset to dataframe
    :return:
    """
    raw_data_file_names = glob.glob('*.sgm')

    entry_arr = []
    for file_name in raw_data_file_names:
        articles = open(file_name).read().split('</REUTERS>')
        for article in articles:
            if '<REUTERS' in article:
                soup = BeautifulSoup(article, 'lxml')

                cgi_split = soup.find('reuters').get('cgisplit')
                lewis_split = soup.find('reuters').get('lewissplit')
                new_id = soup.find('reuters').get('newid')
                topics = soup.find('topics').getText(separator=',').split(',')
                title = None if soup.find('text').find('title') is None else soup.find('text').find(
                    'title').getText().strip()
                date = None if soup.find('text').find('dateline') is None else soup.find('text').find(
                    'dateline').getText().strip()
                raw_text = soup.find('text').contents[-1]

                clean_text = _clean_text(raw_text)

                arr = [new_id, cgi_split, lewis_split, title, date, topics if len(topics[0]) else None, clean_text]

                entry_arr.append(arr)

    reuters_data = pd.DataFrame(entry_arr, columns=['id', 'cgi_split', 'lewis_split', 'title', 'date', 'topics', 'text'])

    return reuters_data


def _clean_text(raw_text):
    """
    clean raw text from reuters data steps:
        1. replace line breaks(\n) with one space
        2. Keep only alphabetical characters and numbers
        3. remove backslashes(\)
        4. reduce multiple spaces to one space
    :param raw_text:
    :return: string without trailing/tailing spaces
    """
    text = raw_text.lower()

    text = re.sub(r'\n', ' ', text)
    text = re.sub(re.compile("[^a-z0-9 ]+"), '', text)
    text = re.sub('\s+', ' ', text)
    text.replace('\\', '')

    return text.strip()


if __name__ == "__main__":
    reuters_data_full = get_reuters_data()
    clean_reuters_data = reuters_data_full[
        (reuters_data_full.text.notnull()) & (~reuters_data_full.text.str.contains('blah blah blah')) & (
            reuters_data_full.topics.notnull()) & (reuters_data_full.text != "")]
    clean_reuters_data.to_pickle('clean_reuters_data.pkl')

