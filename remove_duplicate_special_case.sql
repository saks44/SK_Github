'''
Remove duplicates from table, if a row has value A,B and other B,A, they should be treated as duplicates and A,B should be retained.
--create table `gcp-essentials-saket.COVID.tab` (col1 string, col2 string);
'''
--Insert `gcp-essentials-saket.COVID.tab` (col1,col2) values('A','B'),('A','B'),('B','A'),('A','C')

select distinct col1,col2 from `gcp-essentials-saket.COVID.tab` where  concat(col1,col2) in (select distinct concat(least(col1,col2),greatest(col1,col2)) from `gcp-essentials-saket.COVID.tab`)