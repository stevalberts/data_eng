#Importing Libraries
import requests
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup

#Initialization of known entities
url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_path = '/home/project/top_50_films.csv'
df = pd.DataFrame(columns=["Average Rank","Film","Year","Rotten Tomatoes' Top 100"])
count = 0

#Loading the webpage for Webscraping
html_page = requests.get(url).text
data = BeautifulSoup(html_page,'html.parser')

#Scraping of required information
tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

for row in rows:
    if count < 25:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {"Average Rank":col[0].contents[0], "Film": col[1].contents[0],"Year": col[2].contents[0],"Rotten Tomatoes' Top 100":col[3].contents[0]}
            df1 = pd.DataFrame(data_dict,index=[0])
            df = pd.concat([df,df1],ignore_index=True)
            count+=1
    else:
        break
    
# Convert 'Year' column to numeric type
df['Year'] = pd.to_numeric(df['Year'], errors='coerce')

# Filter for movies from 2000 onwards
df_2000s = df[df['Year'] >= 2000]

print("Films released in the 2000s:")
print(df_2000s)
    
#Storing the data
df.to_csv(csv_path)

conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn,if_exists='replace', index=False)
conn.close()