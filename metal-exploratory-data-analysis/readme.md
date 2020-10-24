Through different processes I tried to manage a exploratory data analysis about metal and all of its subgenres.

Here's the [result](https://datastudio.google.com/s/mStoZ_KrrJc)

As I far as I concern, I don't know a website, or even a database so complete as [The Encyclopaedia Metallum.](metal-archives.com) 
So, to get the data that I needed, I used the scrapper which runs through this website by [garbersc](https://github.com/garbersc/Metal-History-Data-Analysis), who made a new version of the classic one of jonchar (currently not working). I made use of a slightly different version of his code because I want to have the information about country too, so [here's the script with that little change](https://github.com/74minutos/music-data/blob/master/metal-exploratory-data-analysis/metal_scraper.py)

Once I get that data I cleaned up, and denormalize by genre with SQLite and upload the csv file to Datastudio to get the dynamic graphs. The data is updated to October 2020.

The csv file is too large to upload it to github, so if anyone is interested in this database, you can write me to jorgecarrion.l@gmail and I'll send it to you.





