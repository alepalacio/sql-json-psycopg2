# Utilities
import psycopg2
import os
import json

# Connection
connection = psycopg2.connect(
    host='192.168.16.2',
    user='postgres',
    password='example',
    database='openfiles',
    port=5432
)

# Autocommit
connection.autocommit = True


def create_barrio_table():
    cursor = connection.cursor()
    query = """ CREATE TABLE if not exists barrios(id serial primary key, valor varchar(100));"""
    cursor.execute(query)
    cursor.close()


def create_sector_table():
    cursor = connection.cursor()
    query = """ CREATE TABLE if not exists sectores(id serial primary key, valor varchar(100));"""
    cursor.execute(query)
    cursor.close()


def create_puertas_table():
    cursor = connection.cursor()
    query = """CREATE TABLE if not exists puertas(
            id serial primary key,
            numero varchar(50),
            id_sector integer,
            id_barrio integer,
            tipo_uso varchar(5),
            constraint fk_sector foreign key (id_sector) references sectores(id),
            constraint fk_barrio foreign key (id_barrio) references barrios(id)
            );
    
    """
    cursor.execute(query)
    cursor.close()


def create_tables():
    create_barrio_table()
    create_sector_table()
    create_puertas_table()


def insert_barrio(id_barrio):
    cursor = connection.cursor()
    query = f"SELECT * FROM barrios where valor = '{id_barrio}';"
    cursor.execute(query)
    row = cursor.fetchone()

    if row is None:
        query = f"""INSERT INTO barrios(valor) values('{id_barrio}')"""
        cursor.execute(query)

    query = f"SELECT * FROM barrios where valor = '{id_barrio}';"
    cursor.execute(query)
    id_barrio = cursor.fetchone()[0]
    cursor.close()
    return id_barrio


def insert_sector(id_sector):
    cursor = connection.cursor()
    query = f"SELECT * FROM sectores where valor = '{id_sector}';"
    cursor.execute(query)
    row = cursor.fetchone()

    if row is None:
        query = f"""INSERT INTO sectores(valor) values('{id_sector}')"""
        cursor.execute(query)

    query = f"SELECT * FROM sectores where valor = '{id_sector}';"
    cursor.execute(query)
    id_sector = cursor.fetchone()[0]
    cursor.close()
    return id_sector


def insert_puertas(numero, id_sector, id_barrio, tipo_uso):
    cursor = connection.cursor()
    query = f"""INSERT INTO puertas(numero, id_sector, id_barrio, tipo_uso) values(
                '{numero}','{id_sector}','{id_barrio}','{tipo_uso}');"""

    cursor.execute(query)
    cursor.close()


def read_json():
    path = os.getcwd()

    files_list = list()
    with os.scandir(path=path) as files:
        for file in files:
            # print("Name", file.name)
            # print("It's a file", file.is_file())
            # print("Path", file.path)
            # print("It's a directory", file.is_dir())
            # print("""-------------------------------------------------------""")
            if file.is_file() and file.name.split(".")[1] == 'json':
                files_list.append(file.name)

        for i in files_list:
            f = open(i, encoding='utf-8')
            data = json.load(f)
            for j in data:
                print(j["fields"]["numero"])
                id_barrio = insert_barrio(j["fields"]["id_barrio"])
                id_sector = insert_sector(j["fields"]["id_sector"])
                insert_puertas(
                    str(j["fields"]["numero"]),
                    id_sector,
                    id_barrio,
                    j["fields"]["tipo_uso"]
                )
            f.close()
            break


if __name__ == '__main__':
    create_tables()
    read_json()