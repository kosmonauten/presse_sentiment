from flask import Flask, request, jsonify
from config import CONN_STRING
import pandas as pd

import psycopg2

from bs4 import BeautifulSoup
import requests
import spacy
from urllib.parse import urlparse

from spacy import displacy
from collections import Counter

nlp = spacy.load('de')
data = pd.read_csv("./corpora/prepared.csv")

app = Flask(__name__)
conn = ""

SQL_INSERT_ARTICLE = 'INSERT INTO media_sentiment_article (url, text, title, teaser) VALUES (%s, %s, %s, ' \
                     '%s) RETURNING id '

SQL_INSERT_SENT_LIST = 'INSERT INTO media_sentiment_sent_list (word, value, article) VALUES (%s, %s, %s) ' \
                     'RETURNING id '

SQL_INSERT_PANDL_LIST = 'INSERT INTO media_sentiment_persons_locations (entity, type, article) VALUES (%s, %s, %s) ' \
                     'RETURNING id '

SQL_INSERT_PLAYER_ARTICLE = 'INSERT INTO media_sentiment_player (article, player) VALUES (%s, %s) ' \
                     'RETURNING id '

SQL_SELECT_LASTNAME_OF_PLAYER = 'SELECT id FROM player WHERE lastname LIKE %s'

try:
    conn = psycopg2.connect(CONN_STRING)
except:
    print


def persons_and_locations_in_text(text):
    doc = nlp(text)
    data = [(X.text, X.label_) for X in doc.ents]
    df = pd.DataFrame(data, columns=['entity', 'type'])
    return df


def sentiment_in_text(text):
    pd_article = pd.DataFrame(data={'word': text.split(" ")})
    df = pd.merge(pd_article, data, right_on="words", left_on="word")
    return df


def average_sentiment(text):
    df = sentiment_in_text(text)
    return df["value"].mean()


def load_text_from_url(url):
    data = urlparse(url)
    server_loc = ".".join(data.netloc.split(".")[-2:])
    if server_loc == "srf.ch":
        return load_text_from_srf(url)
    elif server_loc == "20min.ch":
        return load_text_from_20min(url)
    elif server_loc == "blick.ch":
        return load_text_from_blick(url)
    elif server_loc == 'bscyb.ch':
        return load_text_from_yb(url)


def load_text_from_yb(url):
    yb = requests.get(url)
    soup = BeautifulSoup(yb.content, 'html.parser')

    title = soup.select('h2')[0].text
    text = soup.select('div.block-1-texts-np')

    text_spider = []

    for t in text:
        text_spider.append(t.text)

    scraped_text = "".join(text_spider)
    return {
        'text': scraped_text,
        'title': title,
        'teaser': '',
    }

def load_text_from_srf(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    title = soup.select('span.article-title__text')[0].text
    text = soup.select('div.article-content p')
    teaser = soup.select('div.article-content ul')

    if len(teaser) > 0:
        teaser = teaser[0]

        t_list = []
        for t in teaser.select('li'):
            t_list.append(t.text.strip())

        teaser = "\n".join(t_list)
    else:
        teaser = ""

    text_spider = []

    for t in text:
        text_spider.append(t.text)

    scraped_text = "".join(text_spider)

    return {
        'text': scraped_text,
        'title': title,
        'teaser': '',
    }


def load_text_from_blick(url):
    blick = requests.get(url)
    soup = BeautifulSoup(blick.content, 'html.parser')

    title = soup.select('span.title')
    text = soup.select('div.article-body p')

    text_spider = []

    if len(title) > 0:
        title = title[0].text
    else:
        title = ""

    for t in text:
        text_spider.append(t.text)

    scraped_text = "".join(text_spider)
    return {
        'text': scraped_text,
        'title': title,
        'teaser': '',
    }

def load_text_from_20min(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    title = soup.select('h1 span')
    text = soup.select('div.story_text p')

    text_spider = []

    if len(title) > 0:
        title = title[0].text

    for t in text:
        text_spider.append(t.text)

    scraped_text = "".join(text_spider)

    return {
        'text': scraped_text,
        'title': title,
        'teaser': '',
    }


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/api/v1/media/add', methods=['PUT'])
def add_report():
    params = request.get_json()
    url = params['url']
    webcontent = load_text_from_url(url)

    df_per_and_loc = persons_and_locations_in_text(webcontent["text"])
    df_sent_list = sentiment_in_text(webcontent['text'])
    webcontent['sentiment_average'] = average_sentiment(webcontent["text"])

    cur = conn.cursor()
    cur.execute(SQL_INSERT_ARTICLE,
                (url, webcontent["text"], webcontent["title"], webcontent["teaser"])
                )

    id_article = cur.fetchone()[0]

    for index, entry in df_sent_list.iterrows():
        cur.execute(SQL_INSERT_SENT_LIST, (entry['words'], entry['value'], id_article))
    for index, entry in df_per_and_loc.iterrows():
        cur.execute(SQL_INSERT_PANDL_LIST, (entry['entity'], entry['type'], id_article))
        ent = entry['entity']
        typ = entry['type']

        # Im Moment sind nur Personen relevant
        if typ == 'PER':
            for possible_name in ent.split(" "):
                cur.execute(SQL_SELECT_LASTNAME_OF_PLAYER, (possible_name,))
                possible_player = cur.fetchone()
                if possible_player is not None:
                    player_id = possible_player[0]
                    # Insert mapping from player_id to id_article into table
                    cur.execute(SQL_INSERT_PLAYER_ARTICLE, (id_article, player_id))
    conn.commit()
    cur.close()
    print(id_article)
    return jsonify(webcontent)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080)
