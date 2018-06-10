# mapreducejoin
This project shows a simple implementation of mapreduce join between two data sets.

As we all know joins are performed to extract relevant data from two data, here's an implementation of joins in MapReduce between two data sets.

This project analyses the highest rating given to a movie by the users. There are two tables Ratings and Movies. Sample data in both tables are: -

Ratings: 
User_id     Movie_Id      Rating
1           233           4
1           501           3
2           501           4
3           432           2.5

Movies:
Movie_Id      Movie_name
233           Iron Man
501           Avengers
432           Raw

Expected Output:

233   IronMan     4
501   Avengers    4
432   Raw         2.5







