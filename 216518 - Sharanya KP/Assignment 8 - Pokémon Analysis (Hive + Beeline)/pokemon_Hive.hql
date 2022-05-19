-- Create Database
--create database if not exists pokemonDB;

-- Use Database
--use pokemonDB;

--drop table if exists pokemon_tbl;

-- Create an External Table
--create external table if not exists pokemon_tbl(id int,Name varchar(50),Type_1 varchar(30),Type_2 varchar(30),Total int,HP int,Attack int,
--Defense int,Sp_Atk int,Sp_Def int,Speed int,Generation int,Legendary varchar(10))
--ROW FORMAT DELIMITED
--FIELDS TERMINATED BY ','
--LINES TERMINATED BY '\n'
--LOCATION '/pokemon'
--tblproperties("skip.header.line.count"="1");

--LOAD DATA LOCAL INPATH '/home/hadoop/Downloads/pokemon.csv' INTO TABLE pokemon_tbl;

--select * from pokemon_tbl limit 5;

-- d) Create and insert values of the existing table into a new table with an additional column power_rate into “powerful”, “moderate” and “powerless” from the table “Pokémon”
--create table pokemon as select *, IF(HP>69.25875, 'powerful', IF(HP<69.25875, 'moderate','powerless')) AS power_rate from pokemon_tbl;
--select COUNT(Name),power_rate from pokemon group by power_rate;

-- c) Find out the average HP (Hit Points) of all the Pokémon
--select avg(HP) from pokemon_tbl;

-- d) Find out top 10 Pokémon according to their HP
--select Name,HP from pokemon_tbl order by HP desc limit 10;

-- e) Find out top 10 Pokémon based on their Attack stat
--select Name,Attack from pokemon_tbl order by Attack desc limit 10;

-- f) Find out top 15 Pokémon based on their defence stat
--select Name,Defense from pokemon_tbl order by Defense desc limit 15;

-- g) Find out the top 20 Pokémon based on their total power
--select Name,Total from pokemon_tbl order by Total desc limit 20;

-- h) Find out the top 10 Pokémon having a drastic change in their attack and sp.attack
--select Name,Attack,Sp_Atk,abs(Sp_Atk-Attack) as atk_diff from pokemon_tbl order by atk_diff desc limit 10;

-- i) Find the top 10 Pokémon having a drastic change in their defence and special defence
--select Name,Defense,Sp_Def,abs(Sp_Def-Attack) as def_diff from pokemon_tbl order by def_diff desc limit 10;
