rem step 1
db "create table tab1(name char(16), quizzes int, midterm int, final int)"
db "create table tab2(college char(20), zipcode char(5), rank int)"

db "insert into tab1 values ('name1', 60, 61, 62)"
db "insert into tab1 values ('name2', 63, 64, 65)"
db "insert into tab1 values ('name3', 66, 67, 68)"
db "insert into tab1 values ('name4', 69, 70, 71)"
db "insert into tab1 values ('name5', 72, 73, 74)"

db "insert into tab2 values ('college1', '94000', 50)"
db "insert into tab2 values ('college2', '94001', 51)"
db "insert into tab2 values ('college3', '94002', 52)"
db "insert into tab2 values ('college4', '94003', 53)"
db "insert into tab2 values ('college5', '94004', 54)"

rem step 2
db "backup to first"

rem step 3
db "insert into tab1 values ('name6', 75, 76, 77)"
db "insert into tab1 values ('name7', 78, 79, 80)"
db "insert into tab1 values ('name8', 81, 82, 83)"
db "insert into tab1 values ('name9', 84, 85, 86)"
db "insert into tab1 values ('name10', 87, 88, 89)"

db "insert into tab2 values ('college6', '94005', 55)"
db "insert into tab2 values ('college7', '94006', 56)"
db "insert into tab2 values ('college8', '94007', 57)"
db "insert into tab2 values ('college9', '94008', 58)"
db "insert into tab2 values ('college10', '94009', 59)"

db "update tab1 set final=199 where name='name1'"
db "delete from tab2 where college='college1'"
db "delete from tab2 where college='college2'"

rem step 4
db "backup to second"
