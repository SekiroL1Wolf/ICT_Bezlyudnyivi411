import pandas
import sqlite3
import csv


def get_domain(link):
    return link.split("://")[1].split("/")[0]


def create(tb, qr, connection='srv.srv'):
    srv = sqlite3.connect(connection)
    srv.create_function("domain_of_url", 1, get_domain)
    srv.execute("create table if not exists " + tb + " as " + qr)
    srv.close()


def save(file, tb, connection='db.db'):
    with open(f"{file}.csv", "w", newline='') as file:
        cursor = sqlite3.connect(connection).cursor()
        writer = csv.writer(file)
        writer.writerow(['id', 'name', 'url', 'domain_of_url'])
        data = cursor.execute("SELECT * FROM " + tb)
        writer.writerows(data)


def execSQL(qr, connection='srv.srv'):
    srv = sqlite3.connect(connection)
    srv.execute(qr)
    srv.commit()
    srv.close()


def load(file, tb, connection='srv.srv'):
    srv = sqlite3.connect(connection)
    pandas.read_csv(f'{file}').to_sql(name=tb, con=srv, if_exists='append', index=False)
    srv.close()