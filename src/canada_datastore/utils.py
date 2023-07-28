import datetime as dt
import logging
from pathlib import Path
from typing import Dict

from qgis.core import (
    QgsApplication,
    QgsContrastEnhancement,
    QgsCoordinateReferenceSystem,
    QgsProject,
    QgsRasterLayer,
    QgsSingleBandGrayRenderer,
    QgsVectorLayer,
)

logger = logging.getLogger("canada_datastore")

URL = "http://10.81.205.13:3333/"
# URL = "/home/jose/data/Canada_datastore/"


def add_qgis_raster_layer(the_url, label):
    raster_layer = QgsRasterLayer(the_url, label)

    # Check if th   e layer was loaded successfully
    if not raster_layer.isValid():
        print(f"Error: Layer {the_url} is not valid.")
        raise IOError

    renderer = QgsSingleBandGrayRenderer(raster_layer.dataProvider(), 1)
    ce = QgsContrastEnhancement(raster_layer.dataProvider().dataType(0))
    ce.setContrastEnhancementAlgorithm(
        QgsContrastEnhancement.StretchToMinimumMaximum
    )
    ce.setMinimumValue(250)
    ce.setMaximumValue(350)
    renderer.setContrastEnhancement(ce)

    raster_layer.setRenderer(renderer)
    return raster_layer


def add_layers_to_project(
    layers1, layers2, layersfirms, project_path: str
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
    layerz = []

    QgsApplication.setPrefixPath("/home/jose/mambaforge/bin", True)
    qgs = QgsApplication([], False, None)
    qgs.initQgis()
    default_crs = QgsCoordinateReferenceSystem("EPSG:3348")
    project = QgsProject.instance()
    project.setCrs(default_crs)  # Set project CRS
    root = project.layerTreeRoot()
    firms_grp = root.addGroup("FIRMS hotspots")
    sorted(layersfirms)
    for group_label, layer_files in layersfirms.items():
        # Add layers to the group based on their type (raster or vector)
        for _, (label, the_url) in layer_files.items():
            print(the_url, label)
            layer = QgsVectorLayer(the_url, f"FIRMS {label}", "ogr")
            layerz.append(layer)
            if layer.isValid():
                print(f"Adding {label}")
                project.addMapLayer(
                    layer, False
                )  # Add the layer without adding it to the layer tree
                firms_grp.addLayer(layer)  # Add the layer
    sorted(layers1)
    lvl1_grp = root.addGroup("S3 Level 1 BT")

    for group_label, layer_files in layers1.items():
        # Add layers to the group based on their type (raster or vector)
        for group_label, [label, the_url] in layer_files.items():
            print(the_url, label)
            layer = add_qgis_raster_layer(the_url, label)
            layerz.append(layer)
            project.addMapLayer(layer, False)
            print(f"Adding {label}")
            lvl1_grp.addLayer(layer)
    sorted(layers2)

    l2a_grp = root.addGroup("S3 Level 2 FRP")
    for _, [label, the_url] in layers2.items():
        layer = QgsVectorLayer(the_url, f"S3 L2A FRP {label}", "ogr")
        print(the_url, label)
        layerz.append(layer)
        if layer.isValid():
            print(f"Adding {label}")
            project.addMapLayer(
                layer, False
            )  # Add the layer without adding it to the layer tree
            l2a_grp.addLayer(layer)
    # Save the project file
    project.write(project_path.as_posix())
    logger.info(f"QGIS project file saved at: {project_path}")
    qgs.exit()


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
        "VIIRS_SNPP_NRT",
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
            tmp[time] = [label, URL + "/".join(fich.parts[-2:])]
        if tmp:
            retval[sensor] = tmp
    return retval


def create_all_project_files(
    lvl1folder, lvl2folder, firmsfolder, output_folder
):
    l1, l2, f1 = scan_layers(lvl1folder, lvl2folder, firmsfolder)
    s1 = set([k.date() for k in l1.keys()])
    s2 = set([k.date() for k in l2.keys()])
    s3 = set([k.date() for k in f1.keys()])
    distinct_dates = sorted(list((s1.union(s2)).union(s3)))

    for today in distinct_dates:
        if today < dt.date(2023, 7, 15):
            continue
        output_fname = (
            output_folder / f"proj_{today.strftime('%Y-%m-%d')}.qgz"
        )
        l1_today = {k: v for k, v in l1.items() if k.date() == today}
        l2_today = {k: v for k, v in l2.items() if k.date() == today}
        firms_today = {k: v for k, v in f1.items() if k.date() == today}
        add_layers_to_project(l1_today, l2_today, firms_today, output_fname)


def scan_layers(lvl1folder, lvl2folder, firmsfolder):
    lvl1folder = get_folder(lvl1folder)
    lvl2folder = get_folder(lvl2folder)
    firmsfolder = get_folder(firmsfolder)
    lvl1_dict = convert_nested_dict(scan_level1(lvl1folder))
    lvl2_dict = scan_level2(lvl2folder)
    firms_dict = convert_nested_dict(scan_firms(firmsfolder))
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


def convert_nested_dict(
    original_dict: Dict[str, Dict[str, int]]
) -> Dict[str, Dict[str, int]]:
    """
    Convert a nested dictionary from the form "dict[sensor][date]" to
    "dict[date][sensor]".

    Args:
        original_dict (Dict[str, Dict[str, int]]): The original nested
        dictionary with sensors and dates.

    Returns:
        Dict[str, Dict[str, int]]: A new nested dictionary with keys
        rearranged, where the outer dictionary
        keys are dates, and the inner dictionary keys are sensors, and the
        values are integers.
    """
    new_dict: Dict[str, Dict[str, int]] = {}

    for sensor, dates_dict in original_dict.items():
        for date, value in dates_dict.items():
            if date not in new_dict:
                new_dict[date] = {}
            new_dict[date][sensor] = value

    return new_dict
