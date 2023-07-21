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
