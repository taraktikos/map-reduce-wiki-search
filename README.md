# Search path between wiki page

# Example results
##William_Shakespeare -> Adolf_Hitler
1. William_Shakespeare -> (First Folio) First_Folio -> (New Orleans) New_Orleans -> (Earl K. Long) Earl_Long -> (Francis Grevemberg) Francis_Grevemberg -> (Allison Kolb) Allison_Kolb -> (Adolf Hitler) Adolf_Hitler
2. William_Shakespeare -> (Grammar schools) Grammar_school  -> (University of York) University_of_York  -> (The Boomtown Rats) The_Boomtown_Rats  -> (A Tonic for the Troops) A_Tonic_for_the_Troops -> (Hitler) Adolf_Hitler
3. William_Shakespeare -> Hamlet -> (best actor) Academy_Award_for_Best_Actor  -> (A Special Day) A_Special_Day -> (Adolf Hitler) Adolf_Hitler
4. William_Shakespeare -> (George Steiner) George_Steiner -> (Hitler) Adolf_Hitler

## Taras_Shevchenko -> Freddie_Mercury
1. Taras_Shevchenko -> 2009_in_film -> O_Lucky_Man! -> Brian_Glover -> Now_That's_What_I_Call_Music! -> Now_That's_What_I_Call_Music_10_(UK_series) -> Freddie_Mercury
2. Taras_Shevchenko -> Zagreb -> David_Bowie -> The_Jean_Genie -> The_Platinum_Collection_(David_Bowie_album) -> Freddie_Mercury
3. Taras_Shevchenko -> 2009_in_film -> Martin_Freeman -> Gavin_Claxton -> Freddie_Mercury
4. Taras_Shevchenko -> Dupont_Circle -> HIV/AIDS_in_the_United_States -> Freddie_Mercury


This map reduce jobs searches shortest path between two wiki page.

Wiki articles dump can download from https://dumps.wikimedia.org/enwiki/20160204/ (enwiki-20160204-pages-articles-multistream.xml)

Result will be in data/SOURCE_PAGE/result/part-r-00000

```
$HADOOP_PREFIX/bin/hadoop jar /WikiHadoop/target/WikiHadoop-1.0-SNAPSHOT.jar wiki.Application input/enwiki-20160204-pages-articles-multistream.xml
```

See result
```
$HADOOP_PREFIX/bin/hdfs dfs -cat data/Taras_Shevchenko/result/*
```
