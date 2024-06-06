Twitter is a popular social media platform that generates a lot of data. This data can be analyzed
to gain insights into user behavior, popular topics, and trends. In this project, two popular data
stores, namely MongoDB and MySQL, were used to store and query the Twitter data efficiently.
MongoDB, a NoSQL database providing high performance and scalability, and MySQL, a
relational database with robust transactional support, were leveraged to combine their
strengths and improve the performance of data storage and retrieval.

To enable quick retrieval of data, indexing was implemented in both databases, and creating
sorted lists of values for a particular field allowed the databases to efficiently search through
large datasets, reducing the time required to retrieve data. The createIndex() method was used
in MongoDB, while the CREATE INDEX statement was used in MySQL to create indexes on the
required columns.

Multiple search functionalities were implemented to search for specific data in the Twitter
dataset, such as keyword search, hashtag search, and user search. Specialized queries were
created to search through specific fields in the Twitter dataset, making searching for particular
data more manageable. To improve the performance of the search application, a cache was
developed for frequently accessed data. By caching frequently accessed data, the number of
queries to the database was reduced, improving the application's overall performance.
