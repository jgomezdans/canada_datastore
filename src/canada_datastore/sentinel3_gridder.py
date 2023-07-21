import datetime as dt
import logging
import subprocess
from pathlib import Path

from osgeo import gdal

logger = logging.getLogger("canada_datastore")

PRODUCT_LUT = {
    "F1_BT_fn.nc": ["F1_BT_fn", "geodetic_fn.nc"],
    "F2_BT_in.nc": ["F2_BT_in", "geodetic_in.nc"],
    "S6_radiance_an.nc": ["S6_radiance_an", "geodetic_an.nc"],
    "S7_BT_in.nc": ["S7_BT_in", "geodetic_in.nc"],
    "S8_BT_in.nc": ["S8_BT_in", "geodetic_in.nc"],
    "S9_BT_in.nc": ["S9_BT_in", "geodetic_in.nc"],
}


def convert_coords(gdal_fname, output_fname) -> None:
    cmd = [
        "gdal_calc.py",
        "-A",
        gdal_fname,
        "--outfile",
        output_fname,
        "--calc",
        "A/1.0e6",
        "--type",
        "Float32",
    ]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing gdal_calc.py: {e}")


def to_vrt(gdal_dataset: str, tag: str) -> Path:
    """Converts GDAL dataset such as 'NETCDF:"geodetic_fn.nc":latitude_fn'
    to

    Args:
        gdal_dataset (str): _description_
        tag (str): _description_

    Returns:
        Path: _description_
    """
    layer = gdal_dataset.lstrip("NETCDF:").split(":")[0]
    layer = layer.replace("'", "")
    layer = layer.replace('"', "")
    layer = layer.replace(".nc", f"_{tag}.vrt")
    # gdal.Translate(layer, gdal_dataset, format="VRT")
    cmd = [
        "gdal_calc.py",
        "-A",
        gdal_dataset,
        "--outfile",
        layer,
        "--calc",
        "A/0.01 + 283.73",
        "--type",
        "Float32",
    ]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing gdal_calc.py: {e}")

    return Path(layer)


def to_tif(product: Path) -> Path:
    if not isinstance(product, Path):
        product = Path(product)
    fname = product.name
    path = product.parent
    layer = PRODUCT_LUT[fname][0]
    geo = path / (PRODUCT_LUT[fname][1])
    tag = PRODUCT_LUT[fname][1].rsplit(".", 1)[0].split("_")[1]
    gdal_dataset = f'NETCDF:"{product}":{layer}'
    lon = f'NETCDF:"{geo}":longitude_{tag}'
    lat = f'NETCDF:"{geo}":latitude_{tag}'
    convert_coords(lon, path / f"longitude_{tag}.tif")
    convert_coords(lat, path / f"latitude_{tag}.tif")
    lon = path / f"longitude_{tag}.tif"
    lat = path / f"latitude_{tag}.tif"
    gdal_dataset = to_vrt(gdal_dataset, "VAL")
    output_file = path / f"{layer}.vrt"
    output_tif = path / f"{layer}.tif"
    geocode_vrt(gdal_dataset.as_posix(), lat, lon, output_file)
    _ = gdal.Warp(
        output_tif.as_posix(),
        output_file.as_posix(),
        format="COG",
        srcSRS="EPSG:4326",
        dstSRS="EPSG:3347",
        xRes=1000,
        yRes=1000,
        creationOptions=["COMPRESS=DEFLATE"],
    )
    return output_tif


def geocode_vrt(
    data_fname: str, lat_file: str, lon_file: str, output_file: str | Path
) -> None:
    g = gdal.Open(data_fname)
    xsize = g.RasterXSize
    ysize = g.RasterYSize
    data_str = f"""<VRTRasterBand band="1" datatype="Float32">
        <SimpleSource>
        <SourceFilename relativeToVRT="1">{data_fname}</SourceFilename>
        <SourceBand>1</SourceBand>
        <SourceProperties RasterXSize="{xsize}" RasterYSize="{ysize}" DataType="Float32"/>
        <SrcRect xOff="0" yOff="0" xSize="{xsize}" ySize="{ysize}" />
        <DstRect xOff="0" yOff="0" xSize="{xsize}" ySize="{ysize}" />
        </SimpleSource>
    </VRTRasterBand>
    """  # noqa: E501
    ds_str = f"""<VRTDataset rasterXSize="{xsize}" rasterYSize="{ysize}">
    <metadata domain="GEOLOCATION">
        <mdi key="X_DATASET">{lon_file}</mdi>
        <mdi key="X_BAND">1</mdi>
        <mdi key="Y_DATASET">{lat_file}</mdi>
        <mdi key="Y_BAND">1</mdi>
        <mdi key="PIXEL_OFFSET">0</mdi>
        <mdi key="LINE_OFFSET">0</mdi>
        <mdi key="PIXEL_STEP">1</mdi>
        <mdi key="LINE_STEP">1</mdi>
    </metadata>
        {data_str}
</VRTDataset>"""
    Path(output_file).write_text(ds_str)


def find_files(path: str | Path) -> dict:
    path = Path(path)
    for k, v in PRODUCT_LUT.items():
        logger.info("Doing " + k)
        to_tif((path / k))
    # # Clean up temporary files
    # for fich in path.glob("*vrt"):
    #     fich.unlink()
    # for fich in path.glob("longitude*tif"):
    #     fich.unlink()
    # for fich in path.glob("latitude*tif"):
    #     fich.unlink()
    fnames = [f for f in path.glob("*.tif")]
    date = gdal.Info((path / "F2_BT_in.nc").as_posix(), format="json")[
        "metadata"
    ][""]["NC_GLOBAL#start_time"]
    date = dt.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")

    return {date: fnames}
