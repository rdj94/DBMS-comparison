# mongodb mysql postgresql comparison
This is the storage that you implemented to experiment with embedded update deletion DML Query and Spatial Query for Geolife Dataset by mysql,postgresql,mongodb.

For implementation, I implemented update, delete, and spatial query excluding the insert part in the code in the repository of those who referred.

## Setup
Prerequisites:

- Python
- Docker-compose

Steps:

1. Make a copy of the `.env-template` file and rename it `.env`
2. Download the dataset from https://www.microsoft.com/en-us/research/publication/geolife-gps-trajectory-dataset-user-guide/ and place the user folders (000-181) in the folder `./data`

## Experiment

To run the experiments with the CLI:

```bash
# Start Docker containers for all DBMSs
docker-compose --compatibility up

# Drop and create SQLite tables for storing experimental results
py cli.py prepare

# Run experiment with desired iterations and total size 
py cli.py run -i 3 -n 5000
or
sh ./run.sh
```

The results are stored in a SQLite database, which can be easily accessed
with Python or a GUI tool like DB Browser.



## Reference

- [Insertion speed of indexed spatial data](https://folk.idi.ntnu.no/baf/eremcis/2022/Group02.pdf)
- [Reference repo](https://github.com/LarsV123/it3010)
- [Geolife dataset](https://www.microsoft.com/en-us/research/publication/geolife-gps-trajectory-dataset-user-guide/)
- [Comparison of Processing PostGIS AND MongoDB](https://edisciplinas.usp.br/pluginfile.php/5530294/mod_resource/content/1/BDAS19_DBAPML_online.pdf)
