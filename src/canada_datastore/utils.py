import datetime as dt
from qgis.core import QgsProject, QgsVectorLayer, QgsRasterLayer
from pathlib import Path
import logging

from typing import Dict, List

logger = logging.getLogger("canada_datastore")

URL = "http://10.81.205.13/shared/jose/canada_datastore/"


def add_layers_to_project(
    layers: Dict[str, List[str]], project_path: str
) -> None:
    """
    Generate a QGIS project file (.qgs) with layers organized into groups
    based on their type (raster or vector).

    Parameters:
        layers (dict): A dictionary where keys represent group labels and
                       values are lists of layer file paths.
                       The layers will be organized under their
                       corresponding group.
        project_path (str): The file path where the QGIS project file will
                       be saved.

    Returns:
        None
    """

    # Initialize QGIS project
    project = QgsProject.instance()
    project.setCrs(
        QgsProject.instance().crs()
    )  # Set project CRS (use the same as the first layer added)

    for group_label, layer_files in layers.items():
        # Create a group for each label
        group = project.layerTreeRoot().addGroup(group_label)

        # Add layers to the group based on their type (raster or vector)
        for layer_file in layer_files:
            if layer_file.lower().endswith(
                ".tif"
            ):  # Check if the layer is a raster
                layer = QgsRasterLayer(
                    layer_file, f"{layer_file.split('.')[0]} - Raster"
                )
            elif layer_file.lower().endswith(
                ".geojson"
            ):  # Check if the layer is a vector
                layer = QgsVectorLayer(
                    layer_file, f"{layer_file.split('.')[0]} - Vector", "ogr"
                )
            else:
                # Skip unsupported file types (you can add more
                # file extensions as needed)
                logger.info(f"Skipping unsupported layer: {layer_file}")
                continue

            if layer.isValid():
                project.addMapLayer(
                    layer, False
                )  # Add the layer without adding it to the layer tree
                group.addLayer(
                    layer
                )  # Add the layer to the corresponding group

    # Save the project file
    project.write(project_path)
    logger.info(f"QGIS project file saved at: {project_path}")


def scan_level1(lvl1folder: Path, bands: list | None = None) -> dict:
    if bands is None:
        bands = ["F1", "F2", "S6", "S7", "S8", "S9"]
    retval = {}
    for band in bands:
        fnames = [
            f
            for f in lvl1folder.rglob(f"**/{band}_BT*tif")
            if f.name.find("VAL") < 0
        ]
        tmp = {}
        for fich in fnames:
            dire = fich.parts[-2]
            sensor = dire[:3]
            time = dt.datetime.strptime(dire.split("_")[7], "%Y%m%dT%H%M%S")
            time_str = time.strftime("%b %d %H:%M")
            label = f"{time_str}_{band}_{sensor}"
            tmp[time] = [label, URL + "/".join(fich.parts[-3:])]
        retval[band] = tmp
    return retval


def scan_level2(lvl2folder: Path) -> dict:
    folder = lvl2folder / "processed_output"
    fnames = [f for f in folder.glob("*.geojson")]
    retval = {}
    for fich in fnames:
        time = dt.datetime.strptime(fich.name.split("_")[7], "%Y%m%dT%H%M%S")
        time_str = time.strftime("%b %d %H:%M")
        label = f"{time_str}"
        retval[time] = [label, URL + "/".join(fich.parts[-3:])]
    return retval


def scan_firms(firmsfolder: Path) -> dict:
    sensors = [
        "MODIS_NRT",
        "VIIRS_NOAA20_NRT",
        "VIIR_NPP_NRT",
        "GOES_NRT",
    ]
    retval = {}
    for sensor in sensors:
        fnames = [f for f in firmsfolder.glob(f"{sensor}*.geojson")]
        tmp = {}
        for fich in fnames:
            time = dt.datetime.strptime(
                fich.name.split("_")[-1].rstrip(".geojson"), "%Y-%m-%d"
            )
            time_str = time.strftime("%b %d")
            label = f"{time_str} {sensor.rstrip('_NRT')}"
            tmp[time] = [label, URL + "/".join(fich.parts[-3:])]
        if tmp:
            retval[sensor] = tmp
    return retval


def scan_layers(lvl1folder, lvl2folder, firmsfolder):
    lvl1folder = get_folder(lvl1folder)
    lvl2folder = get_folder(lvl2folder)
    firmsfolder = get_folder(firmsfolder)
    lvl1_dict = scan_level1(lvl1folder)
    lvl2_dict = scan_level2(lvl2folder)
    firms_dict = scan_firms(firmsfolder)
    return (lvl1_dict, lvl2_dict, firms_dict)


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


def get_folder(folder: str | Path | None) -> Path | None:
    if folder is not None:
        folder = Path(folder)
        if not folder.exists():
            raise IOError(f"Can't find {folder} product folder")
        return folder
    return folder
