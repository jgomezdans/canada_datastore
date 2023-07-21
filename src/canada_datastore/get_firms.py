import json
import logging
import pandas as pd
import datetime as dt
from pathlib import Path

logger = logging.getLogger("canada_datastore")

FIRMS_KEY = "325418b740118263076006b1225f097a"


def dataframe_to_geojson(df):
    df["time"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%s")
    # Create a GeoJSON Feature Collection structure
    geojson = {"type": "FeatureCollection", "features": []}

    # Iterate through the DataFrame and create GeoJSON features for each row
    for index, row in df.iterrows():
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row["longitude"], row["latitude"]],
            },
            "properties": {
                k: row[k]
                for k in row.keys()
                if k not in ["acq_date", "acq_time"]
            },
        }
        geojson["features"].append(feature)
    return geojson


def get_firms_date(
    date: None | dt.datetime,
    output_folder: str | Path,
    region: None | list = None,
    range: int = 1,
    sensors: None | list = None,
):
    if sensors is None:
        sensors = [
            "MODIS_NRT",
            "VIIRS_NOAA20_NRT",
            "VIIR_NPP_NRT",
            "GOES_NRT",
        ]
    output_folder = (
        Path(output_folder)
        if not isinstance(output_folder, Path)
        else output_folder
    )
    if region is None:
        region = [-98, 46, -82, 56]
    region = f"{region[0]},{region[1]},{region[2]},{region[3]}"
    if date is None:
        start_date = dt.datetime.now()
    else:
        start_date = date
    start_date = date.strftime("%Y-%m-%d")
    dfs = {}
    for sensor in sensors:
        url = (
            "https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
            + f"{FIRMS_KEY}/{sensor}/{region}/{range}/{start_date}"
        )
        df = pd.read_csv(url)
        if not df.empty:
            # Have a simple timestamp column...
            df["acq_time"] = df["acq_time"].astype(str).str.zfill(4).values
            df["acq_time"] = (
                df["acq_time"].str.slice(stop=2)
                + ":"
                + df["acq_time"].str.slice(start=2)
                + ":00"
            )

            df["acq_date"] = pd.to_datetime(df["acq_date"])
            df["time"] = df["acq_date"] + pd.to_timedelta(df["acq_time"])
            df.to_csv(
                output_folder / f"{sensor}_{start_date}.csv", index=False
            )
            dfs[sensor] = df
            geojson = dataframe_to_geojson(df)
            with open(
                output_folder / f"{sensor}_{start_date}.geojson", "w"
            ) as outfile:
                json.dump(geojson, outfile)

        else:
            logger.info(f"No hotspots for {sensor}...")
    return dfs
