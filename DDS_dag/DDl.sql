
create table vlada_test.test_employees (
ID int, -- ID сотрудника
NAME1 text, -- Фамилия сотрудника
NAME2 text, -- Имя сотрудника
NAME3 text -- Отчество сотрудника
)


create table vlada_test.test_salary (
ID int, -- ID сотрудника
dt date, -- Дата выплаты
Salary_Type text, -- Тип суммы (salary, bonus)
Amount float -- выплаченная сумма
)

create table vlada_test.test_emails (
ID int, -- ID Email
Empl_ID int, -- ID сотрудника
Email text -- Email адрес
)


insert into vlada_test.test_employees 
values 
    (1, 'Шершуков', 'Виктор', 'Кузьмич'),
    (2, 'Битова', 'Анастасия', 'Юрьевна'),
    (3, 'Кириллов', 'Валентин', 'Владиславович'),
    (2, 'Битова', 'Анастасия', 'Юрьевна'),
    (3, 'Кириллов', 'Валентин', 'Владиславович'),
    (4, 'Игнатьев', 'Игорь', 'Дмитриевич')
    

insert into vlada_test.test_emails 
values 
    (1, 1, 'shershuko@mail.ru'),
    (2, 1, 'shershuko-v@mail.ru'),
    (3, 2, 'bitova@mail.ru'),
    (4, 2, 'bitova@mail.ru'),
    (5, 3, 'kirillov@mail.ru'),
    (6, 3, 'kirillov@mail.ru'),
    (5, 3, 'kirillov@mail.ru'),
    (6, 3, 'kirillov@mail.ru')


insert into vlada_test.test_salary
values 
 (1, '2019-12-01', 'salary', 50000),
    (1, '2020-01-01', 'salary', 50000),
    (1, '2020-02-01', 'salary', 50000),
    (1, '2020-03-01', 'salary', 50000),
    (1, '2020-04-01', 'salary', 50000),
    (1, '2020-05-01', 'salary', 50000),
    (1, '2020-06-01', 'salary', 53000),
    (1, '2020-07-01', 'salary', 53000),
    (1, '2020-08-01', 'salary', 53000),
    (1, '2020-09-01', 'salary', 53000),
    (1, '2020-10-01', 'salary', 53000),
    (1, '2020-11-01', 'salary', 53000),
    (1, '2020-12-01', 'salary', 53000),
    (1, '2021-01-01', 'salary', 53000),
    (1, '2019-12-01', 'bonus', 10000),
    (1, '2020-01-01', 'bonus', 10000),
    (1, '2020-02-01', 'bonus', 9000),
    (1, '2020-03-01', 'bonus', 11000),
    (1, '2020-04-01', 'bonus', 10000),
    (1, '2020-05-01', 'bonus', 5000),
    (1, '2020-06-01', 'bonus', 10000),
    (1, '2020-07-01', 'bonus', 10000),
    (1, '2020-08-01', 'bonus', 10000),
    (1, '2020-09-01', 'bonus', 10000),
    (1, '2020-10-01', 'bonus', 10000),
    (1, '2020-11-01', 'bonus', 10000),
    (1, '2020-12-01', 'bonus', 10000),
    (1, '2021-01-01', 'bonus', 10000),
    (2, '2019-12-01', 'salary', 60000),
    (2, '2020-01-01', 'salary', 60000),
    (2, '2020-02-01', 'salary', 60000),
    (2, '2020-03-01', 'salary', 62000),
    (2, '2020-04-01', 'salary', 62000),
    (2, '2020-05-01', 'salary', 62000),
    (2, '2020-06-01', 'salary', 62000),
    (2, '2020-07-01', 'salary', 62000),
    (2, '2020-08-01', 'salary', 62000),
    (2, '2020-09-01', 'salary', 62000),
    (2, '2020-10-01', 'salary', 65000),
    (2, '2020-11-01', 'salary', 65000),
    (2, '2020-12-01', 'salary', 65000),
    (2, '2021-01-01', 'salary', 65000),
    (2, '2019-12-01', 'bonus', 10000),
    (2, '2020-01-01', 'bonus', 10000),
    (2, '2020-02-01', 'bonus', 9000),
    (2, '2020-03-01', 'bonus', 11000),
    (2, '2020-04-01', 'bonus', 10000),
    (2, '2020-05-01', 'bonus', 5000),
    (2, '2020-06-01', 'bonus', 10000),
    (2, '2020-07-01', 'bonus', 7000),
    (2, '2020-08-01', 'bonus', 7000),
    (2, '2020-09-01', 'bonus', 7000),
    (2, '2020-10-01', 'bonus', 7000),
    (2, '2020-11-01', 'bonus', 7000),
    (2, '2020-12-01', 'bonus', 7000),
    (2, '2021-01-01', 'bonus', 7000),
    (3, '2019-12-01', 'salary', 60000),
    (3, '2020-01-01', 'salary', 60000),
    (3, '2020-02-01', 'salary', 60000),
    (3, '2020-03-01', 'salary', 60000),
    (3, '2020-04-01', 'salary', 60000),
    (3, '2020-05-01', 'salary', 60000),
    (3, '2020-06-01', 'salary', 60000),
    (3, '2020-07-01', 'salary', 60000),
    (3, '2020-08-01', 'salary', 60000),
    (3, '2020-09-01', 'salary', 60000),
    (3, '2020-10-01', 'salary', 60000),
    (3, '2020-11-01', 'salary', 64000),
    (3, '2020-12-01', 'salary', 64000),
    (3, '2021-01-01', 'salary', 64000),
    (4, '2019-12-01', 'salary', 61000),
    (4, '2020-01-01', 'salary', 61000),
    (4, '2020-02-01', 'salary', 61000),
    (4, '2020-03-01', 'salary', 61000),
    (4, '2020-04-01', 'salary', 61000),
    (4, '2020-05-01', 'salary', 63000),
    (4, '2020-06-01', 'salary', 63000),
    (4, '2020-07-01', 'salary', 63000),
    (4, '2020-08-01', 'salary', 63000),
    (4, '2020-09-01', 'salary', 63000),
    (4, '2020-10-01', 'salary', 63000),
    (4, '2020-11-01', 'salary', 63000),
    (4, '2020-12-01', 'salary', 63000),
    (4, '2021-01-01', 'salary', 63000),
    (4, '2019-12-01', 'bonus', 7000),
    (4, '2020-01-01', 'bonus', 7000),
    (4, '2020-02-01', 'bonus', 7000),
    (4, '2020-03-01', 'bonus', 7000),
    (4, '2020-04-01', 'bonus', 7000),
    (4, '2020-05-01', 'bonus', 7000),
    (4, '2020-06-01', 'bonus', 7000),
    (4, '2020-07-01', 'bonus', 7000),
    (4, '2020-08-01', 'bonus', 7000),
    (4, '2020-09-01', 'bonus', 7000),
    (4, '2020-10-01', 'bonus', 7000),
    (4, '2020-11-01', 'bonus', 7000),
    (4, '2020-12-01', 'bonus', 7000),
    (4, '2021-01-01', 'bonus', 7000)




create table vlada_test.test_employees_dds (
ID int, -- ID сотрудника
NAME1 text, -- Фамилия сотрудника
NAME2 text, -- Имя сотрудника
NAME3 text -- Отчество сотрудника
)


create table vlada_test.test_salary_dds (
ID int, -- ID сотрудника
dt date, -- Дата выплаты
Salary_Type text, -- Тип суммы (salary, bonus)
Amount float -- выплаченная сумма
)

create table vlada_test.test_emails_dds (
ID int, -- ID Email
Empl_ID int, -- ID сотрудника
Email text -- Email адрес
)

create table vlada_test.test_emails_dds (
Empl_ID int, -- ID Email
FIO text, -- ID сотрудника
Salary text, 
Bonus text, 
Email text -- Email адрес
)

create table vlada_test.final_report (
Empl_ID int, -- ID Email
FIO text, -- ID сотрудника
Salary text, 
Bonus text, 
Email text -- Email адрес
)