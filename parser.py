import os
from tqdm import tqdm


def get_paths() -> list[tuple[int, str]]:
    """
    Find all .plt files in data set.
    Returns a list of tuples, with username as integer and full path for each
    file.
    """
    filepaths = []
    for root, _, files in sorted(os.walk("data")):
        for filename in files:
            if filename.endswith(".plt"):
                path = os.path.join(root, filename)
                username = path.split("/")[1]
                filepaths.append((int(username), path))

        
    return filepaths


def read_data(max_records=None, max_folders=None) -> list:
    """
    Read up to MAX_FOLDERS .plt files from disk and parse them into a standard
    format.
    """
    folders = get_paths()
    if max_folders is not None:
        folders = folders[:max_folders]

    data = []
    progress = tqdm(total=len(folders), leave=False)
    row_count = 0
    for username, path in folders:
        if max_records and row_count >= max_records:
            break
        progress.set_description(f"Processing {path}")
        file = open(path, "r")
        rows = file.readlines()[6:]  # Skip header
        rows = [x.strip().split(",") for x in rows]
        for row in rows:
            if max_records and row_count >= max_records:
                break
            latitude = float(row[0])
            longitude = float(row[1])
            altitude = float(row[3])
            date = row[5]
            time = row[6]
            data.append([username, latitude, longitude, altitude, date, time])
            row_count += 1
        file.close()
        progress.update(1)
    return data


def pg_parse(data: list):
    """
    Parse a list of preprocessed rows and convert them into a format suited to
    the Postgres version of the schema.
    """
    pg_data = []
    for user, lat, lon, alt, date, time in data:
        if lon <-180 or lon > 180:
            continue
        if lat < -90 or lat > 90:
            continue
        # Note order swapping: (lat, lon) => (lon, lat)
        pg_data.append([user, lon, lat, alt, date, time])
    return pg_data


def mysql_parse(data: list):
    """
    Parse a list of preprocessed rows and convert them into a format suited to
    the MySQL version of the schema.
    """
    my_data = []
    for user, lat, lon, alt, date, time in data:
        if lon <-180 or lon > 180:
            continue
        if lat < -90 or lat > 90:
            continue
        my_data.append([user, lon, lat, alt, date, time])
    return my_data


def mongo_parse(data: list,size: int= None):
    """
    Parse a list of preprocessed rows and convert them into a format suited to
    the MondoDB version of the schema.
    """
    mongo_data = []
    n = 1
    for user, lat, lon, alt, date, time in data:
        # Note order swapping: (lat, lon) => (lon, lat)
        if lon <-180 or lon > 180:
            continue
        if lat < -90 or lat > 90:
            continue
        mongo_data.append(
            {
                "tp_user": user,
                "tp_point": [lon, lat],
                "tp_altitude": alt,
                "tp_date": date,
                "tp_time": time,
                'tp_id': n + size if size else n
            }
        )
        n += 1
    return mongo_data
