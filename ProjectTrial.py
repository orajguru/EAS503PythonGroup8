#!/usr/bin/env python
# coding: utf-8

# In[1]:


from IPython.display import display, HTML
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows


# In[2]:


def normalize_database():
    
    conn_norm = create_connection('covid.db')
    cur_norm  = conn_norm.cursor()
    
    execute_sql_statement('DROP TABLE IF EXISTS StatewiseVaccination;',conn_norm)
    execute_sql_statement('DROP TABLE IF EXISTS StatewiseDeath;',conn_norm)
    execute_sql_statement('DROP TABLE IF EXISTS State;',conn_norm)
    execute_sql_statement('DROP TABLE IF EXISTS StateDate;',conn_norm)
    

    data_filename_1 = "USDeath.csv"
    data_filename_2 = "USVaccination.csv"
    with open(data_filename_2, 'r') as file:
        ls2 = []
        for line in file:
            ls2.append(line.split(","))
    cd = []
    stcd = []
    for i in range(1,len(ls2)):
        if ls2[i][15] != "":
            if ls2[i][15] not in cd:
                cd.append(ls2[i][15])
                tup1 = (ls2[i][15], ls2[i][1])
                stcd.append(tup1)

        
    with open(data_filename_1, 'r') as file:
        ls = []
        dt = []
        for line in file:
            ls.append(line.split(","))
        state = []
        for i in range(1,len(ls)):
            if ls[i][1] not in state:
                state.append(ls[i][1])
        state.sort()
        for i in range(1,len(ls)):
            mdy = ls[i][0].split("/")
            if int(mdy[2]) == 21:
                if 1 <= int(mdy[0]) <= 9:
                    mdy[0] = str(mdy[0]).zfill(2)
                if 1 <= int(mdy[1]) <= 9:
                    mdy[1] = str(mdy[1]).zfill(2)
                newdt = mdy[0]+mdy[1]+mdy[2]
                if newdt not in dt:
                    dt.append(newdt)
        dt.sort() 
        stdt = []
        for i in range(1, len(dt)):
            tup = (i,dt[i-1])
            stdt.append(tup)
        tup = (337, "120321")
        stdt.append(tup)
        with conn_norm:
            sql_create_st = "CREATE TABLE State(StateID TEXT NOT NULL PRIMARY KEY, StateName TEXT NOT NULL);"
            create_table(conn_norm, sql_create_st)
            sql_insert_st = "INSERT INTO State(StateID, StateName) VALUES(?, ?)"
            cur_norm.executemany(sql_insert_st, stcd)
            sql_statement_1 = "SELECT * FROM State WHERE StateID = 'NY' LIMIT 1"
            df1 = pd.read_sql_query(sql_statement_1, conn_norm)
            display(df1)

            sql_create_st_dt = "CREATE TABLE StateDate(StateDateID TEXT NOT NULL PRIMARY KEY, SubDate TEXT NOT NULL);"
            create_table(conn_norm, sql_create_st_dt)
            sql_insert_st_dt = "INSERT INTO StateDate(StateDateID, SubDate) VALUES(?, ?)"
            cur_norm.executemany(sql_insert_st_dt, stdt)
            sql_statement_2 = "SELECT * FROM StateDate WHERE StateDateID = '84' LIMIT 1;"
            df2 = pd.read_sql_query(sql_statement_2, conn_norm)
            display(df2)
        

        stdeath = []
        for i in range(1,len(ls)):
            mdy2 = ls[i][0].split("/")
            if int(mdy2[2]) == 21:
                if 1 <= int(mdy2[0]) <= 9:
                    mdy2[0] = str(mdy2[0]).zfill(2)
                if 1 <= int(mdy2[1]) <= 9:
                    mdy2[1] = str(mdy2[1]).zfill(2)
                newdt2 = mdy2[0]+mdy2[1]+mdy2[2]
                if ls[i][1] in cd:
                    with conn_norm:
                        sql_statement_3 = "SELECT * FROM StateDate WHERE SubDate = '"+newdt2+"';"
                        exn = execute_sql_statement(sql_statement_3, conn_norm)
                        tup2 = (exn[0][0], ls[i][1], int(ls[i][2]), int(ls[i][5]), int(ls[i][7]), int(ls[i][10])) 
                        stdeath.append(tup2)

    stvac = [] 
    for i in range(1,len(ls2)):
            mdy3 = ls2[i][0].split("/")
            if int(mdy3[2]) == 21:
                if 1 <= int(mdy3[0]) <= 9:
                    mdy3[0] = str(mdy3[0]).zfill(2)
                if 1 <= int(mdy3[1]) <= 9:
                    mdy3[1] = str(mdy3[1]).zfill(2)
                newdt3 = mdy3[0]+mdy3[1]+mdy3[2]
                if ls2[i][15] in cd:
                    with conn_norm:
                        sql_statement_6 = "SELECT * FROM StateDate WHERE SubDate = '"+newdt3+"';"
                        exn = execute_sql_statement(sql_statement_6, conn_norm)
                        tup3 = (exn[0][0], ls2[i][15], ls2[i][4], ls2[i][7], ls2[i][2], ls2[i][11]) 
                        
                        stvac.append(tup3)

    
    with conn_norm:
        sql_create_st_death = "CREATE TABLE StatewiseDeath(StateDateID TEXT NOT NULL, StateID TEXT NOT NULL, NewCases INTEGER, TotalCases INTEGER, NewDeaths INTEGER, TotalDeaths INTEGER, FOREIGN KEY (StateDateID) REFERENCES StateDate(StateDateID), FOREIGN KEY (StateID) REFERENCES State(StateID));"
        create_table(conn_norm, sql_create_st_death)
        sql_insert_st_death = "INSERT INTO StatewiseDeath(StateDateID, StateID, TotalCases, NewCases, TotalDeaths, NewDeaths) VALUES(?, ?, ?, ?, ?, ?)"
        cur_norm.executemany(sql_insert_st_death, stdeath)
        sql_statement_4 = "SELECT * FROM StatewiseDeath"
        df4 = pd.read_sql_query(sql_statement_4, conn_norm)
        display(df4)
        
        sql_create_st_vac = "CREATE TABLE StatewiseVaccination(StateDateID TEXT NOT NULL, StateID TEXT NOT NULL, PeopleVaccinated INTEGER, PeopleFullyVaccinated INTEGER, TotalVaccinated INTEGER, DailyVaccinated INTEGER, FOREIGN KEY (StateDateID) REFERENCES StateDate(StateDateID), FOREIGN KEY (StateID) REFERENCES State(StateID));"
        create_table(conn_norm, sql_create_st_vac)
        sql_insert_st_vac = "INSERT INTO StatewiseVaccination(StateDateID, StateID, PeopleVaccinated, PeopleFullyVaccinated, TotalVaccinated, DailyVaccinated) VALUES(?, ?, ?, ?, ?, ?)"
        cur_norm.executemany(sql_insert_st_vac, stvac)
        sql_statement_5 = "SELECT * FROM StatewiseVaccination"
        df4 = pd.read_sql_query(sql_statement_5, conn_norm)
        display(df4)





    conn_norm.commit()

normalize_database()


# In[3]:


import streamlit as st
import numpy as np
from PIL import Image
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go


# In[4]:


st.title('Covid Vaccination vs Death Rate')
st.write('Please get vaccinated')
st.sidebar.title("Selector")
image = Image.open("Vaccine.jpeg")
st.image(image, use_column_width = True)
st.markdown('<style>body{background-color: pink;}</style>', unsafe_allow_html = True)


# In[5]:


#Loading Data
@st.cache
def load_data(filename):
    df = pd.read_csv(filename)
    return df
df_vac = load_data("USVaccination.csv")
df_death = load_data("USDeath.csv")


# In[1]:


#use a selection option
visualization = st.sidebar.selectbox('Select a Chart type', ('Bar Chart', 'Pie Chart'))
state_select = st.sidebar.selectbox('Select a state', df_vac['state'].unique())
selected_date = st.sidebar.date_input('Select a Date')
newdt = selected_date.strftime('%m/%d/%Y')
mdy = newdt.split("/")
if int(mdy[2]) == 2021:
    mdy[2] = '21'
    if 1 <= int(mdy[0]) <= 9:
        mdy[0] = str(mdy[0]).zfill(2)
    if 1 <= int(mdy[1]) <= 9:
        mdy[1] = str(mdy[1]).zfill(2)
    newdt = mdy[0]+mdy[1]+mdy[2]
# mdy[2] = "21"
# mdy = mdy[0]+"/"+mdy[1]+"/"+mdy[2]
# print(mdy)
# print(df_vac['date'])
# print(df_death['submission_date'])
print(newdt)
print(state_select)
conn_norm = create_connection('normalized.db')
cur_norm  = conn_norm.cursor()
with conn_norm:
        sql_statement_sel_date_id = "SELECT * FROM StateDate WHERE SubDate ="+newdt
        df_sql_date_id = pd.read_sql_query(sql_statement_sel_date_id, conn_norm)
        #display(df_sql_date_id[0]['StateDateID'])
        display(df_sql_date_id['StateDateID'][0])

        
#         sql_statement_sel_state_id = "SELECT StateID FROM State WHERE StateName = '"+state_select+"'"
#         df_sql_state_id = pd.read_sql_query(sql_statement_sel_state_id, conn_norm)
#         display(df_sql_state_id)
        sql_statement_sel_vac = "SELECT * FROM StatewiseVaccination WHERE StateDateID ="+str(df_sql_date_id['StateDateID'][0])+" AND StateID ='"+state_select+"'"
        print(sql_statement_sel_vac)
        df_sql_vac = pd.read_sql_query(sql_statement_sel_vac, conn_norm)
        display(df_sql_vac)
        sql_statement_sel_death = "SELECT * FROM StatewiseDeath WHERE StateDateID ="+str(df_sql_date_id['StateDateID'][0])+" AND StateID ='"+state_select+"'"
        print(sql_statement_sel_death)
        df_sql_death = pd.read_sql_query(sql_statement_sel_death, conn_norm)
        display(df_sql_death)
        
        sql_statement_pie_vac = "SELECT * FROM StatewiseDeath WHERE StateDateID ="+str(df_sql_date_id['StateDateID'][0])
        df_sql_vac_pie = pd.read_sql_query(sql_statement_pie_vac, conn_norm)

        sql_statement_pie_death = "SELECT * FROM StatewiseDeath WHERE StateDateID ="+str(df_sql_date_id['StateDateID'][0])
        df_sql_death_pie = pd.read_sql_query(sql_statement_pie_death, conn_norm)
        
#selected_state_vac = df_vac[df_vac['state'] == state_select & df_vac['date'] == mdy]

status_select = st.sidebar.radio('COVID - 19 US', ('Total Vaccinations','Total Deaths'))
#selected_state_death = df_death[df_death['state'] == state_select & df_death['submission_date'] == mdy]
st.markdown("## **State level analysis**")


# In[1]:


def get_total_dataframe(df_vac, df_death):
    total_dataframe = pd.DataFrame({
    'Parameter': ['TotalVaccinated', 'PeopleVaccinated', 'PeopleFullyVaccinated', 'TotalDeaths'],
    'Number of people': (df_vac.iloc[0]['TotalVaccinated'],
    df_vac.iloc[0]['PeopleVaccinated'],
    df_vac.iloc[0]['PeopleFullyVaccinated'],
    df_death.iloc[0]['TotalDeaths'])})
    return total_dataframe
state_total = get_total_dataframe(df_sql_vac, df_sql_death)
def get_daily_dataframe(df_vac, df_death):
    daily_dataframe = pd.DataFrame({
    'Parameter': ['DailyVaccinated', 'NewDeaths'],
    'Number of people': (df_vac.iloc[0]['DailyVaccinated'],
    df_death.iloc[0]['NewDeaths'])})
    return daily_dataframe
state_daily = get_daily_dataframe(df_sql_vac, df_sql_death)
if visualization=='Bar Chart':
    state_daily_graph = px.bar(state_daily, x='Parameter', y='Number of people', 
                           labels={'Number of people': 'Number of people in %s' % (state_select)}, color='Parameter')
    st.plotly_chart(state_daily_graph)
    state_total_graph = px.bar(state_total, x='Parameter', y='Number of people', 
                           labels={'Number of people': 'Number of people in %s' % (state_select)}, color='Parameter')
    st.plotly_chart(state_total_graph)
elif visualization=='Pie Chart':
    if status_select=='Total Vaccinations':
        st.title("Total Vaccinations")
        fig = px.pie(df_vac, values=df_vac['total_vaccinations'], names=df_vac['state'])
        st.plotly_chart(fig)
    elif status_select=='Total Deaths':
        st.title("Total Deaths")
        fig = px.pie(df_death, values=df_death['tot_death'], names=df_death['state'])
        st.plotly_chart(fig)


# In[ ]:




