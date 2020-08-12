import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
from random import randint
from headers import headers


orig_df = pd.read_csv('/Users/jana1394/Desktop/Studium/Master/SS2020/DatenanalyseI/TermPaper/book-price-crawler/assets/input/books-2.csv', error_bad_lines=False)

isbns = orig_df['isbn'].values

price_dict = {'isbn': [],
              'price': []}

for isbn in tqdm(isbns):
    header_int = randint(0, len(headers)-1)
    price_dict['isbn'].append(isbn)

    successful = False
    r_count = 0
    while not successful:

        rquest = requests.get('https://isbndb.com/book/{}'.format(isbn), headers={'User-Agent': headers[header_int]})

        if rquest.status_code != 200:
            sleep_time = randint(20, 30)
            print('Too many requests: Retrying in {} seconds'.format(sleep_time))
            time.sleep(sleep_time)
            r_count += 1
        else:
            successful = True
            r_count = 0

        if r_count == 2:
            header_int = randint(0, len(headers) - 1)
            r_count = 0

    book_content = BeautifulSoup(rquest.content, features="html.parser")

    try:
        price = book_content.find_all('td')[-1].text.strip('USD ')
        if '$' not in price:
            raise AttributeError
    except AttributeError as e:
        print('Price Not Found')
        price = ''
    
    except IndexError:
        print("Wrong Index")
        price = ''
        pass

    print(price)
    price_dict['price'].append(price)

prices_df = pd.DataFrame.from_dict(price_dict)

final_df = orig_df.merge(prices_df, how='left', left_on='isbn', right_on='isbn')
final_df.to_csv('/Users/jana1394/Desktop/Studium/Master/SS2020/DatenanalyseI/TermPaper/book-price-crawler/assets/output/goodreads.csv', index=False)
