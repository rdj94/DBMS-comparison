# Insertion speed of indexed spatial data

This Github repository holds the code used by group 2 in the course IT3010 in 2022.
This was used to generate data for our paper, the full title of which is:

> Insertion speed of indexed spatial data: comparing MySQL, PostgreSQL and MongoDB

## Contributors

- Lars-Olav Vågene
- Ingvild Løver Thon
- Eirik Schøien
- Christian Axell
- Lukas Tveiten

## Setup

Prerequisites:

- Python
- Docker

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
```

The results are stored in a SQLite database, which can be easily accessed
with Python or a GUI tool like DB Browser.
# mongodb-vs-mysql-vs-postgresql
