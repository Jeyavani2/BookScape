#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import mysql.connector
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
import PIL.Image
PIL.Image.MAX_IMAGE_PIXELS = 933120000

def main_frame():
    #st.set_page_config(page_title="BookScape Explorer", page_icon="📚", layout="wide")
    st.title("📖 _:orange[Books Data Analysis]_")

    st.header("📚 :blue[Books Database]", divider="rainbow")
    st.subheader(":rainbow[Introduction:]")
    st.write("""BookScape Explorer is an advanced data analytics project that brings together the power of the Google Books API and data visualization to create a comprehensive book analysis platform. This project consists of two main components: a data extraction system and an interactive analytics dashboard.""")
    st.subheader(":rainbow[What Does This Project Do?]")
    st.write("""The project serves two main purposes:

1.Data Collection: It harvests detailed book information from Google Books API across multiple categories like Programming, Data Science, Literature, etc.""")
    st.write("""2.Data Analysis: It provides an interactive dashboard where users can explore and analyze this book data through various visualizations and metrics.""")
    st.write("""
        **:rainbow[Tools & Technologies Used:]**
        - **Books API**: For fetching books data directly from Google Books API.
        - **Python**: Utilized for data extraction, cleaning, and analysis.
        - **SQL**: Used for storing and querying the cleaned dataset.
        - **Streamlit**: For building a real-time, interactive dashboard to display insights.
    """)
    st.subheader(":rainbow[Project Flow]")
    st.write(""" **:blue[API Integration:]**
    - Connects to Google Books API
    - Fetches comprehensive book data
    - Handles API rate limiting and pagination
    - Processes JSON responses""")
    st.write(""" **:blue[Database Management:]**

- Creates a normalized MySQL database structure
- Handles data cleaning and validation
- Manages relationships between different entities (authors, publishers, etc.)
- Implements error handling for database operations""")
    st.write(""" **:blue[Data Processing:]**

- Extracts relevant information from API responses
- Normalizes data across different tables
- Handles missing or inconsistent data
- Creates proper relationships between different data entities""")
    st.write(""" **:blue[Streamlit Application (Analytics Dashboard)]:**
   - This is the visualization and analysis component that provides an interactive web interface.""")
#--------------------------------------------------------------------------------------------------------------   
def get_db_connection():
    return mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "jeya",
    database="bookscape",        
    auth_plugin='mysql_native_password'

)
#------------------------------------------------    
def ebook(): 
    connection=get_db_connection()
    cursor=connection.cursor()
    query="""select book_title,case when isEbook=1 then "Ebook" else "Physicalbook" end as ebook_physicalbook from book_data order by    ebook_physicalbook"""
    cursor.execute(query)
    result=cursor.fetchall()
    cursor.close()
    connection.close()
    df=pd.DataFrame(result,columns=["BOOK_TITLE","EBOOK OR PHYSICAL BOOK"])
    return df
#--------------------------------------------------------
def ebookpercent():
    connection=get_db_connection()
    cursor=connection.cursor()
    query="""select count(*),case when isEbook=1 then "Ebook" else "Physicalbook" end as ebook_physicalbook from book_data group by ebook_physicalbook"""
    cursor.execute(query)
    result=cursor.fetchall()
    cursor.close()
    connection.close()
    df=pd.DataFrame(result,columns=["count","EBOOK OR PHYSICAL BOOK"])
    return df
#------------------------------------------------------------
def most_book_publisher():
    connection=get_db_connection()
    cursor=connection.cursor()
    query="""select publisher,count(publisher) as bookspublished from authors_publishers  where publisher!="" group by publisher order by bookspublished desc limit 1"""
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Publisher','Total Books Published'])
    return df
#-------------------------------------------------------------------
#def publisher_with_highest_average_rating():
def Top5_most_expensive_Bboks_by_Retail_Price():    
    connection=get_db_connection()
    cursor=connection.cursor()
    query="""select book_id,book_title,amount_retailPrice from book_data join price on book_data.new_id=price.new_id order by amount_retailPrice desc limit 5"""
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Book_Id','Book Title','Amount Retail Price'])
    return df
#---------------------------------------------------------------------------------------
def publisher_with_highest_average_rating():
    connection=get_db_connection()
    cursor=connection.cursor()
    query=""" select publisher,max(averageRating) as maxrating from authors_publishers,rating where authors_publishers.new_id=rating.new_id and publisher != '' group by publisher order by maxrating desc limit 1"""
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Publisher','Highest Average Rating'])
    return df
def  Books_Published_After_2010_with_atLeast_500Pages():
    connection=get_db_connection()
    cursor=connection.cursor()
    query=""" select book_id,book_title,pageCount,year from book_data where year > 2010 and pageCount > 500 order by year desc"""
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Book_Id','Book_Title','PageCount','Year'])
    return df 
  #-------------------------------------------------------------------------------
def List_Books_with_Discounts_Greater_than_20_percentage():
    connection=get_db_connection()
    cursor=connection.cursor()
    query=""" select book_id,book_title,amount_listPrice,amount_retailPrice,round((amount_listPrice-amount_retailPrice),2) as discount,round(((amount_listPrice-amount_retailPrice) * 100)/amount_listprice,2) as discountpercentage from book_data,price where book_data.new_id=price.new_id having discountpercentage > 20 order by discountpercentage desc"""
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Book_Id','Book_Title','Amount List Price','Amount Retail Price','Discount','Discount Percentage'])
    return df 
#----------------------------------------
def Top3_Authors_with_the_Most_Books():
    connection=get_db_connection()
    cursor=connection.cursor()
    query=""" select book_authors,count(authors_publishers.new_id) as sumbooks from authors_publishers  where  book_authors!='' group by book_authors order by sumbooks desc limit 3"""
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Book Authors','No. of Books'])
    return df 
#-------------------------------------------------------------------------------
def Average_PageCount_for_eBooks_vs_Physical_Books():
    connection=get_db_connection()
    cursor=connection.cursor()
    query=""" select case when isEbook = 1 then 'Ebook' else 'Physicalbook' end as ebookk,round(avg(pageCount),2) as avgpagecount from book_data group by ebookk"""
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['BookType','Avg PageCount'])
    return df
#------------------------------------------------
def Publishers_with_More_than_10_Books():
    connection=get_db_connection()
    cursor=connection.cursor()
    query=""" select publisher,count(authors_publishers.new_id) as sumbooks from authors_publishers  where  publisher!=''  group by publisher having sumbooks > 10 order by sumbooks desc """
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Publisher','Books Published'])
    return df
#------------------------------------------------------
def Average_PageCount_for_Each_Category():
    connection=get_db_connection()
    cursor=connection.cursor()
    query="""  select categories,round(avg(pageCount),2) as avgpagecount from book_data group by categories order by avgpagecount desc """
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Categories','Avg PageCount'])
    return df
#-------------------------------------------------
def Books_with_More_than_3_Authors():
    connection=get_db_connection()
    cursor=connection.cursor()
    query=""" SELECT book_title,book_authors,(length(book_authors)-length(replace(book_authors,',','')))+1 as count  from  book_data,authors_publishers where authors_publishers.new_id=book_data.new_id  having count > 3 order by count desc """
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Book Title','Book Authors','No. of Authors'])
    return df
#----------------------------------------------------
def Books_with_Ratings_Count_Greater_Than_the_Average():
    connection=get_db_connection()
    cursor=connection.cursor()
    query=""" select book_title,ratingsCount,averageRating from book_data,rating where book_data.new_id=rating.new_id and ratingsCount>averageRating order by ratingsCount"""
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Book Title','Ratings Count','Average Rating'])
    return df
#------------------------------------------------------------------
def Books_with_the_Same_Author_Published_in_the_Same_Year():
    connection=get_db_connection()
    cursor=connection.cursor()
    query="""  select book_authors ,year,count(*) as books  from authors_publishers,book_data where book_data.new_id=authors_publishers.new_id and book_authors != ''   group by book_authors,year having books > 1 and year != '' order by  year desc"""
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Book Authors','Year','Books Published'])
    return df
#----------------------------------------------------------
def Books_with_a_Specific_Keyword_in_the_Title(searchkey):
    connection=get_db_connection()
    cursor=connection.cursor()
    query=""" select book_title,categories,pageCount,language,country,saleability,isEbook,buyLink,year from book_data where """ + searchkey
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Book Title','Categories','pageCount','language','country','saleability','isEbook','buyLink','year'])
    return df
#--------------------------------------------------------------
def Year_with_the_Highest_Average_Book_Price():
    connection=get_db_connection()
    cursor=connection.cursor()
    query="""  select year,round(avg(amount_retailPrice),2) as avgbookprice from price,book_data where book_data.new_id=price.new_id group by year order by year desc """ 
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Year','Avg Book Price'])
    return df
#---------------------------------------------------------------------
def Count_Authors_Who_Published_3_Consecutive_Years():
    connection=get_db_connection()
    cursor=connection.cursor()
    cursor.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
    query=""" with cte as (select authors_publishers.book_authors,year from authors_publishers,book_data where authors_publishers.new_id=book_data.new_id and authors_publishers.book_authors!='' and year !=''  group by authors_publishers.book_authors,year  order by authors_publishers.book_authors,year)SELECT DISTINCT p1.book_authors
FROM cte p1,
     cte p2
WHERE (p1.year = p2.year + 1 OR p1.year = p2.year - 1) AND p1.book_authors = p2.book_authors group by p1.book_authors having count(p1.book_authors) > 3
ORDER BY p1.book_authors, p1.year""" 
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Book Authors'])
    return df
#-------------------------------------------------------------------
def Authors_who_have_published_books_in_the_same_year_but_under_different_publishers():
    connection=get_db_connection()
    cursor=connection.cursor()
    query="""  with cte as (select  distinct(publisher) ,book_authors,year from authors_publishers,book_data where authors_publishers.new_id=book_data.new_id and book_authors != '' and publisher != '' and year != ''  order by year)select book_authors,year,count(*) from cte  group by book_authors,year  having count(*) > 1   order by year """ 
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Book Authors','Year','No. of Books'])
    return df
#-------------------------------------------------------------------
def Average_amount_retailPrice_of_eBooks_and_physicalbooks():
    connection=get_db_connection()
    cursor=connection.cursor()
    query=""" select case when isEbook=1 then "Ebook" else "Physicalbook" end as ebook_physicalbook ,round(avg(amount_retailPrice),2) from book_data,price where book_data.new_id=price.new_id   group by ebook_physicalbook order by    ebook_physicalbook""" 
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['EBook/PhysicalBook','Avg. amount of Retail Price'])
    return df
#-------------------------------------------------------------------
def AverageRating_that_is_more_than_two_standard_deviations_away_from_the_average_rating_of_all_books():
    connection=get_db_connection()
    cursor=connection.cursor()
    cursor.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
    query="""  with cte as (select 2*stddev(averageRating) as std from rating) select book_title,averageRating, ratingsCount,cte.std from rating,cte,book_data where book_data.new_id=rating.new_id and averageRating > cte.std group by rating.new_id order by averageRating desc""" 
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Book Title','Avg. amount Rating','Ratings Count','2 Std'])
    return df
#-------------------------------------------------------------------------------
def Publisher_has_the_highest_average_rating_among_its_books_but_only_for_publishers_that_have_published_more_than_10_books():
    connection=get_db_connection()
    cursor=connection.cursor()
    query="""   select publisher,max(averageRating),count(book_title) as numbooks from authors_publishers,book_data,rating where authors_publishers.new_id=book_data.new_id  and authors_publishers.new_id=rating.new_id  and publisher != ''  group by publisher having   count(book_title) > 10 order by numbooks desc""" 
    cursor.execute(query)
    result=cursor.fetchall()
    df=pd.DataFrame(result,columns=['Publisher','Max Avg. Rating','No. of Books'])
    return df
#--------------------------------------------------------------------------------------------------------------------------------------------------------------
intro = st.sidebar.radio('Main Menu',["Introduction","Check In for BookScape Explorer Analysis"])
if  intro=='Introduction':    
    main_frame()  
elif intro == "Check In for BookScape Explorer Analysis":  
    st.title("📖 _:orange[Books Data Analysis]_")

    st.header("📚 :blue[Books Database]", divider="rainbow")

    #st.subheader("📚 Books Database")
    
    st.write("""
        🚀 **Ready to explore the data?** Use the interactive tools below to filter and analyze the data in real-time!
    """)
    selected_question = st.selectbox(
            "**_:rainbow[Select a Question to View Analysis]_**",
            ["1.Check Availability of eBooks vs Physical Books",
             "2.Find the Publisher with the Most Books Published",
             "3.Identify the Publisher with the Highest Average Rating",
             "4.Get the Top 5 Most Expensive Books by Retail Price",
             "5.Find Books Published After 2010 with at Least 500 Pages",
             "6.List Books with Discounts Greater than 20%",
             "7.Find the Average Page Count for eBooks vs Physical Books",
             "8.Find the Top 3 Authors with the Most Books",
             "9.List Publishers with More than 10 Books",
             "10.Find the Average Page Count for Each Category",
             "11.Retrieve Books with More than 3 Authors",
             "12.Books with Ratings Count Greater Than the Average",
             "13.Books with the Same Author Published in the Same Year",
             "14.Books with a Specific Keyword in the Title",
             "15.Year with the Highest Average Book Price",
             "16.Count Authors Who Published 3 Consecutive Years",
             "17.Authors who have published books in the same year but under different publishers",
             "18.Average amount_retailPrice of eBooks and physical books",
             "19.AverageRating that is more than two standard deviations away from the average rating of all books",
             "20.Publisher has the highest average rating among its books, but only for publishers that have published more than 10 books"
             ],index=None
        )
    if selected_question == "1.Check Availability of eBooks vs Physical Books":
        
              a=pd.DataFrame(ebook())
              a.index.name="Index"
             
             
              st.write(a)
              chart_container = st.container()
              with chart_container: 
                  b=pd.DataFrame(ebookpercent())
                  figp, axp = plt.subplots()
                  axp.pie(b['count'],labels=b['EBOOK OR PHYSICAL BOOK'],autopct='%1.1f%%',startangle=90)
                  ##figp=px.pie(b['count'],b['EBOOK OR PHYSICAL BOOK'],labels={'x':'x axis','y':'y axis'})
                  #figp.show()
                  axp.axis('equal')
                  st.write('---')
                  st.header(':rainbow[Pie chart for Availability of eBooks vs Physical Books]')
                  
                  #figp=px.pie(a['EBOOK OR PHYSICAL BOOK'],a['BOOK_TITLE'],labels={'x':'x axis','y':'y axis'})
                  
                  st.pyplot(figp) 
                  figp.clear()
                  axp.clear()
                  plt.close(figp)
                  del figp
                 # fig3 = go.Figure(data=[go.Pie(labels=b['EBOOK OR PHYSICAL BOOK'], values=(b['count'])])  # Replace with your chart type and data
                  fig3 = go.Figure(data=[go.Pie(labels=b['EBOOK OR PHYSICAL BOOK'], values=b['count'])])  # Replace with your chart type and data
                  #fig3.update_layout(title='plot Pie chart for Availability of eBooks vs Physical Books')
                  st.header(':rainbow[PLotly Pie chart for Availability of eBooks vs Physical Books]')
                  st.plotly_chart(fig3, use_container_width=True)
    elif   selected_question == "2.Find the Publisher with the Most Books Published":
              a=pd.DataFrame(most_book_publisher())
              a.index.name='Index'
                 
              st.write(a) 
    elif   selected_question == "3.Identify the Publisher with the Highest Average Rating":
              a=pd.DataFrame(publisher_with_highest_average_rating())
              a.index.name='Index'
                 
              st.write(a)    
    elif   selected_question == "4.Get the Top 5 Most Expensive Books by Retail Price":
              a=pd.DataFrame(Top5_most_expensive_Bboks_by_Retail_Price())
              a.index.name='Index'
                 
              st.write(a)   
              #fig2 = go.Figure(data=[go.Bar(x=a['Book Title'], y=a['Amount Retail Price'])])  # Replace with your chart type and data
                  #fig2.update_layout(title="Chart 2")
              fig2 = go.Figure(data=[go.Bar(x=a['Book Title'], y=a['Amount Retail Price'],marker=dict(color=("#00ffff"))  )],layout=dict(xaxis_title="Book Title",yaxis_title="Amount Retail Price")) 
                  
              st.header(':rainbow[Plotly Bar chart for Top 5 Most Expensive Books by Retail Price]')
              st.plotly_chart(fig2, use_container_width=True)
              fig, ax = plt.subplots(1,1,figsize=(50,50),dpi=50)
                  
           
              st.header(':rainbow[Bar chart for Top 5 Most Expensive Books by Retail Price]')
           

# Display the chart in Streamlit
              ax.bar(a['Book Title'],a['Amount Retail Price'],color='#FFFF00')
            

# Set text color
              #ax.set_facecolor='yellow' 
              ax.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
              ax.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
              ax.set_xlabel('Book_Title', color='#000080',fontsize=180)
              ax.set_ylabel('Amount Retail Price', color='#000080',fontsize=180)
              st.pyplot(fig)
                  #plt.clf()
              fig.clear()
              ax.clear()
              plt.close(fig)
              del fig
                 
    elif   selected_question == "5.Find Books Published After 2010 with at Least 500 Pages":
              a=pd.DataFrame(Books_Published_After_2010_with_atLeast_500Pages())
              a.index.name='Index'
                 
              st.write(a)  
    elif   selected_question == "6.List Books with Discounts Greater than 20%":
              a=pd.DataFrame(List_Books_with_Discounts_Greater_than_20_percentage())
              a.index.name='Index'
                 
              st.write(a)    
              fig2 = go.Figure(data=[go.Bar(x=a['Book_Title'], y=a['Discount Percentage'])])  # Replace with your chart type and data
              #fig2.update_layout(title="Chart 2",color='red')
                  
              st.header(':rainbow[Bar chart for  Books with Discounts Greater than 20%]')
              st.plotly_chart(fig2, use_container_width=True)
    elif   selected_question == "7.Find the Average Page Count for eBooks vs Physical Books":
              a=pd.DataFrame(Average_PageCount_for_eBooks_vs_Physical_Books())
              a.index.name='Index'  
              st.write(a)  
              chart_container = st.container()
              with chart_container:
                  figp, axp = plt.subplots()
                  axp.pie(a['Avg PageCount'],labels=a['BookType'],autopct='%1.1f%%',startangle=90)
                  axp.axis('equal')
                  st.write('---')
                  st.header(':rainbow[Pie chart for Average Page Count for eBooks vs Physical Books]')            
                  st.pyplot(figp) 
                  figp.clear()
                  axp.clear()
                  plt.close(figp)
                  del figp
    elif   selected_question == "8.Find the Top 3 Authors with the Most Books":
              a=pd.DataFrame(Top3_Authors_with_the_Most_Books())
              a.index.name='Index'   
              st.write(a)         
              #chart_container = st.container()
              #with chart_container:  
              fig, ax = plt.subplots(1,1,figsize=(50,50),dpi=50)        
              st.header(':rainbow[Bar chart for  Top 3 Authors with the Most Books]')
           

# Display the chart in Streamlit
              ax.bar(a['Book Authors'],a['No. of Books'],color='#00ffff')
            

# Set text color
              
              ax.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
              ax.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
              ax.set_xlabel('Book Authors', color='#000080',fontsize=180)
              ax.set_ylabel('No. of Books', color='#000080',fontsize=180)
              st.pyplot(fig)
              fig.clear()
              ax.clear()
              plt.close(fig)
              del fig
                  
    elif   selected_question == "9.List Publishers with More than 10 Books":
              a=pd.DataFrame(Publishers_with_More_than_10_Books())
              a.index.name='Index'   
              st.write(a)    
              #chart_container = st.container()
              #with chart_container:
              fig2 = go.Figure(data=[go.Bar(x=a['Publisher'], y=a['Books Published'],marker=dict(color=("#00ffff")) )],layout=dict(xaxis_title="Publisher",yaxis_title="Books Published"))  # Replace with your chart type and data
              #fig2.update_layout(color='red')
                  
              st.header(':rainbow[Bar chart for Publishers with More than 10 Books]')
              st.plotly_chart(fig2, use_container_width=True)
    elif   selected_question == "10.Find the Average Page Count for Each Category":
              a=pd.DataFrame(Average_PageCount_for_Each_Category())
              a.index.name='Index'   
              st.write(a)  
   
    elif   selected_question == "11.Retrieve Books with More than 3 Authors":
              a=pd.DataFrame(Books_with_More_than_3_Authors())
              a.index.name='Index'   
              st.write(a)   
    elif   selected_question == "12.Books with Ratings Count Greater Than the Average":
              a=pd.DataFrame(Books_with_Ratings_Count_Greater_Than_the_Average())
              a.index.name='Index'   
              st.write(a)  
              fig2 = go.Figure(data=[go.Bar(x=a['Book Title'], y=a['Ratings Count'],marker=dict(color=("#00ffff")) )],layout=dict(xaxis_title="Book Title",yaxis_title="Ratings Count"))  # Replace with your chart type and data
              #fig2.update_layout(color='red')
                  
              st.header(':rainbow[Bar chart for Books with Ratings Count Greater Than the Average]')
              st.plotly_chart(fig2, use_container_width=True)
    elif   selected_question == "13.Books with the Same Author Published in the Same Year":
              a=pd.DataFrame(Books_with_the_Same_Author_Published_in_the_Same_Year())
              a.index.name='Index'   
              st.write(a)  
    elif   selected_question == "14.Books with a Specific Keyword in the Title":
              st.subheader("_:rainbow[Enter Specific Keyword in the Title :]_")
              keyss=st.text_input("")
              keyword=[]
              if keyss != '':
                  keyword=keyss.split(',')
                  l=1
                  for i in keyword:
                          if l == 1 :
                               if l == len(keyword):
                                   res="book_title  like '%" + i + "%'"
                               else:   
                                   res = "book_title  like '%" + i + "%'" + " or " 
                          else:      
                               if l == len(keyword):                 
                                   res+= "book_title  like '%" + i + "%'"
                             
                               else:
                                   res += "book_title  like '%" + i + "%'" + " or " 
                          l+=1   
                  #st.write("res",res)
                  a=pd.DataFrame(Books_with_a_Specific_Keyword_in_the_Title(res))
                  a.index.name='Index'   
                  st.write(a)
    elif   selected_question == "15.Year with the Highest Average Book Price":
              a=pd.DataFrame(Year_with_the_Highest_Average_Book_Price())
              a.index.name='Index'   
              st.write(a)   
    elif   selected_question == "16.Count Authors Who Published 3 Consecutive Years":
              a=pd.DataFrame(Count_Authors_Who_Published_3_Consecutive_Years())
              a.index.name='Index'   
              st.write(a)  
              
          
              
    elif   selected_question == "17.Authors who have published books in the same year but under different publishers":
              a=pd.DataFrame(Authors_who_have_published_books_in_the_same_year_but_under_different_publishers())
              a.index.name='Index'   
              st.write(a) 
    elif   selected_question == "18.Average amount_retailPrice of eBooks and physical books":
              a=pd.DataFrame(Average_amount_retailPrice_of_eBooks_and_physicalbooks())
              a.index.name='Index'   
              st.write(a) 
    elif   selected_question == "19.AverageRating that is more than two standard deviations away from the average rating of all books":
              a=pd.DataFrame(AverageRating_that_is_more_than_two_standard_deviations_away_from_the_average_rating_of_all_books())
              a.index.name='Index'   
              st.write(a)
    elif   selected_question == "20.Publisher has the highest average rating among its books, but only for publishers that have published more than 10 books":
              a=pd.DataFrame(Publisher_has_the_highest_average_rating_among_its_books_but_only_for_publishers_that_have_published_more_than_10_books())
              a.index.name='Index'   
              st.write(a)     
              
              