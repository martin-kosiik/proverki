import sqlite3
from sqlite3 import Error
import os
import xml.etree.ElementTree as ET

os.chdir(r'C:\Users\marti\OneDrive\Plocha\research_projects\proverky')

tree = ET.parse('xml_files\data-20200403-structure-20180523.xml')
root = tree.getroot()

tree = ET.parse('xml_files\data-20201006-structure-20180523.xml')

root = tree.getroot()

len(root.getchildren())

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    create_connection(r"db\pythonsqlite.db")
