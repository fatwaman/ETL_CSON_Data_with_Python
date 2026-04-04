# 📊 **ETL Data to PostgreSQL using Python**
Previously I've been using KNIME to support my daily data entry tasks. Now I try to make small experiment by comparing it with Python from raw to database and verification

### 🗃️ **Dataset**
- I'm using data which have ~370k rows per day
- In this experiment, I used 4 days of data

----------

### 📚 **Data Ingestion flow:**
The flow itself is not too complex:

1. **Put data in defined folder**

   KNIME/Python will check and extract the file(s) which have specific filename

2. **Data trasnformation**

   In this part, the transformation process is simple
   - Change the data format to "𝘥𝘥/𝘮𝘮/𝘺𝘺"
   - arrange column names
   - sort the data

3. **Running PostgreSQL engine and connect to database**

   Setting up the database environment and connect KNIME/Python to interact with the database

4. **Running PostgreSQL engine and connect to database**

   Storing data in the defines tables and verifying the upload result

5. **Data verification**

   Make verification using line chart to check the data already import

Your data is successfully ingested into database 🎉

----------

### 📊 **Result from experiment**
- As on video, KNIME Analytics need 115s and Python need 87s to ingest data from raw to data verification. Python is ~25% more faster than KNIME
- The advantage of using KNIME is that we are not required to master coding because the way to operate it is simply "drag and drop"
- Whereas by Python, we at least need to have the ability to understand the algorithm which are then translated into coding script
<div align="center">
  <img src="https://github.com/fatwaman/ETL_CSON_Data_with_Python/blob/master/Result_Compare_KNIME_vs_Python.jpg" height="600" alt="compare Knime vs Python"  />
  <img width="600" />
</div>

---------

### 🔆 **Lesson learned from this experiment**

The lessons learned were more focused on creating Python code. I encountered many errors, but from here we'll learn how to fix them. For example:

- Data types differ between the extracted results and the database
  
   **Solution:** Match the data types

- If follow method from the previous post, it takes a long time because it sends the data to the database one row at a time (or in very small chunks), which creates a lot of "network chatter" between Python and your Postgres

   **Solution:** Create a function that uses Postgres' built-in COPY command. This is the fastest way to move data into Postgres
     ```bash
     # Function to use PostgreSQL's COPY command for faster bulk inserts
     
     import csv
     from io import StringIO
     
     def psql_insert_copy(table, conn, keys, data_iter):
         # gets a DBAPI connection from the sqlalchemy engine
         dbapi_conn = conn.connection
         with dbapi_conn.cursor() as cur:
             s_buf = StringIO()
             writer = csv.writer(s_buf)
             writer.writerows(data_iter)
             s_buf.seek(0)
     
             columns = ', '.join('"{}"'.format(k) for k in keys)
             if table.schema:
                 table_name = '{}.{}'.format(table.schema, table.name)
             else:
                 table_name = table.name
     
             sql = 'COPY "{}" ({}) FROM STDIN WITH CSV'.format(table_name, columns) # Add double quotes around table name to handle special characters or reserved keywords
             cur.copy_expert(sql=sql, file=s_buf)
     ```  

- Found a 𝘔𝘦𝘮𝘰𝘳𝘺𝘌𝘳𝘳𝘰𝘳 message, due to converting large columns and regular expressions consumes a lot of RAM

   **Solution:** Use "Chunking," breaking rows into smaller "chunks". This way, your RAM only has to handle one small chunk of data at a time, but you still get the high speed of the COPY method
     ```bash
     # 1. Define your chunk size (100k rows is usually safe for 16GB RAM)
     chunk_size = 100000
     
     # 2. Loop through the dataframe in pieces
     for i in range(0, len(df), chunk_size):
         # Select the current chunk
         df_chunk = df.iloc[i : i + chunk_size]
         
         # Upload this chunk using the fast COPY method
         df_chunk.to_sql(
             name="4G_CSON_KPI_202603", 
             con=engine, 
             if_exists="append", 
             index=False, 
             method=psql_insert_copy
         )
         
         print(f"Successfully uploaded rows {i} to {min(i + chunk_size, len(df))}")
     
     print("--- All Data Uploaded Successfully ---")
     ```  
----------
