Data mart is being created to facilitate their Data Science team to work on multiple loyalty management use cases. Data is being pulled from multiple diverse data sources and co-located them in a centralized global database (Amazon Redshift database). 
Our 3-step approach for this ETL pipeline is: 
i.	Pull data from MySQL Server, Amazon S3 bucket & internal SFTP and put them into Huggies S3 bucket acting as staging area. 
ii.	Create Spark Job to process above data and storing them subsequent intermediate stages. 
iii.	Load the data into Amazon Redshift. 
	