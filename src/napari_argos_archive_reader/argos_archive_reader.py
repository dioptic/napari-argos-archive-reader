from __future__ import annotations

import io
import typing
import zipfile
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path

import dask.array as da
import numpy as np
from dask import delayed
from pydantic import BaseModel
from pydantic import __version__ as pydantic_version
from ruamel.yaml import YAML
from skimage.io import imread

X_DIR = -1
Y_DIR = 1

UM2MM = 1e-3
MM2UM = 1e3

LEGACY_PYDANTIC = bool(pydantic_version.startswith("1."))


class Axes(BaseModel):
    shape: tuple[int, int]


class AxesXY(Axes):
    um_per_px: float
    center_px: tuple[float, float] = (0, 0)
    mirror_y: bool = False


class ZStackMetadata(BaseModel):
    z_mm: float
    z_stack_mm: typing.Sequence[float]


class MatrixIlluminationMetadata(BaseModel):
    led_config: int
    led_configs: typing.Sequence[int]
    exposure_time_us: float
    exposure_times_us: typing.Optional[typing.Sequence[float]] = None


class ArchiveLayer(BaseModel):
    """Structured data container holding information about
    a single argos layer stored in an archive.
    """

    archive_file: str  #: Path to the .zip file that is the ARGOS image archive
    image: str  #: file name of the image corresponding to this layer
    axes_xy: AxesXY
    z: float = 0.0
    binary: typing.Optional[
        str
    ] = None  #: file name of the binary segementation mask for this layer
    mask_geometry: typing.Optional[typing.Any] = None
    z_stack: typing.Optional[tuple[float, ...]] = None
    illumination_metadata: typing.Optional[MatrixIlluminationMetadata] = None


@dataclass
class StackInfo:
    """Structured data container for stacks and
    associated metadata. Holds all the information
    we need to create a LayerDataTuple for napari
    """

    stack: typing.Union[da.Array, np.ndarray]
    segmentation: typing.Optional[typing.Union[da.Array, np.ndarray]] = None
    name: typing.Optional[str] = None
    translate: typing.Optional[typing.Sequence[float]] = None
    scale: typing.Optional[typing.Sequence[float]] = None
    argos_archive_file: typing.Optional[str] = None
    metadata: typing.Optional[dict[str, typing.Any]] = None


def image_for_layer(
    layer: ArchiveLayer,
    reader: typing.Callable = imread,
    segmentation: bool = False,
):
    zip_path = zipfile.Path(layer.archive_file)
    image_file_in_zip = (
        (zip_path / layer.binary) if segmentation and layer.binary else (zip_path / layer.image)
    )
    return reader(io.BytesIO(image_file_in_zip.read_bytes()))


def read_descriptor_yml(zip_path: zipfile.Path) -> dict:
    """Deserializes the descriptor.yml file contained in the .zip archive
    into a python dictionary"""
    yaml = YAML(typ="safe")
    yaml_text = (zip_path / "descriptor.yml").read_text()
    return yaml.load(yaml_text)


def parse_layer_dict(ld, archive_file: typing.Union[Path, str]):
    assert ld["axes"][0] == "AxesXY", "Only AxesXY supported!"
    axes_xy_dict = ld["axes"][1]
    axes_xy = (
        AxesXY.parse_obj(axes_xy_dict) if LEGACY_PYDANTIC else AxesXY.model_validate(axes_xy_dict)
    )
    mask_geometry = None  # TODO
    z_stack = ld.get("z_stack", None)
    metadata = ld.get("metadata", None)
    illumination_dict = metadata.get("MatrixIlluminationMetadata", None)
    if illumination_dict is None:
        illumination = None
    else:
        illumination = (
            MatrixIlluminationMetadata.parse_obj(illumination_dict)
            if LEGACY_PYDANTIC
            else MatrixIlluminationMetadata.model_validate(illumination_dict)
        )
    return ArchiveLayer(
        archive_file=str(Path(archive_file)),
        image=ld["image"],
        axes_xy=axes_xy,
        mask_geometry=mask_geometry,
        binary=ld.get("binary", None),
        z=ld["z"],
        z_stack=z_stack,
        illumination_metadata=illumination,
    )


def parse_archive_descriptor_dict(
    descriptor: dict, archive_file: typing.Union[Path, str]
) -> list[ArchiveLayer]:
    layer_dictionaries = descriptor["ArgosArchiveSource"]["layers"]
    return [parse_layer_dict(layer_dict, archive_file) for layer_dict in layer_dictionaries]


def layers_to_dask_array(layers: typing.Sequence[ArchiveLayer], segmentation: bool = False):
    assert len(layers), "Need at least one layer!"
    layer_shape = layers[0].axes_xy.shape
    # assert all layer shapes are the same
    assert all(
        layer.axes_xy.shape == layer_shape for layer in layers
    ), "All layers must have the same shape!"
    layer_dtype = np.uint8

    return da.stack(
        [
            da.from_delayed(
                delayed(image_for_layer(layer, reader=imread, segmentation=segmentation)),
                shape=layer_shape,
                dtype=layer_dtype,
            )
            for layer in layers
        ],
    )


def read_group(
    grouping_key,
    layers: typing.Sequence[ArchiveLayer],
    argos_archive_file: typing.Union[Path, str],
) -> StackInfo:
    """Reads a sequence of ARGOS layers and returns a NapariStackInfo object

    Args:
        grouping_key: _description_
        layers: _description_
        argos_archive_file: _description_

    Returns:
        _description_
    """
    name = Path(argos_archive_file).stem

    # Add the objects that have been used for grouping as metadata
    metadata: dict[str, typing.Any] = {}
    for item in grouping_key:
        metadata[type(item).__name__] = item

    illumination_meta = list(
        filter(lambda x: isinstance(x, MatrixIlluminationMetadata), grouping_key)
    )
    if illumination_meta:
        name += f"_{illumination_meta[0].led_config}_{illumination_meta[0].exposure_time_us:.1f}"

    stack = layers_to_dask_array(layers)

    if layers[0].binary is not None:
        segmentation = layers_to_dask_array(layers, segmentation=True)
    else:
        segmentation = None

    translate_um = [0.0, 0.0, 0.0]
    scale_um = [1.0, 1.0, 1.0]

    axes = layers[0].axes_xy
    scale_um[1] = axes.um_per_px
    scale_um[2] = axes.um_per_px

    y_dir = -1 * Y_DIR if axes.mirror_y else Y_DIR
    translate_um[1] = axes.um_per_px * (y_dir * axes.center_px[1])
    translate_um[2] = axes.um_per_px * (X_DIR * axes.center_px[0])

    if stack.ndim == 3 and stack.shape[0] > 1 and layers[0].z_stack is not None:
        z_positions = layers[0].z_stack
        translate_um[0] = min(z_positions) * MM2UM
        # TODO: assert all layers equidistantly spaced in Z (equal spacing is not required by ARGOS,
        # but probably always the case)
        scale_um[0] = abs(z_positions[1] - z_positions[0]) * MM2UM if len(z_positions) > 1 else 1.0

    return StackInfo(
        stack=stack,
        segmentation=segmentation,
        translate=translate_um,
        scale=scale_um,
        argos_archive_file=str(argos_archive_file),
        name=name,
        metadata=metadata,
    )


def read_argos_archive(archive_file: typing.Union[Path, str]) -> typing.List[StackInfo]:
    zip_path = zipfile.Path(archive_file)

    descriptor = read_descriptor_yml(zip_path)

    version = descriptor["ArgosArchiveSource"].get("version", 1)

    if version == 1:
        return _read_argos_archive_v1(archive_file, descriptor)
    elif version == 2:
        return _read_argos_archive_v2(archive_file, descriptor)
    raise RuntimeError(f"Unsupported ARGOS archive version {version}.")


def _read_argos_archive_v1(
    archive_file: typing.Union[Path, str], descriptor: dict, reader: typing.Callable = imread
) -> typing.List[StackInfo]:
    """Minimal support for v1 (non-matrix) files. Simply stack all image files
    in zip folder without much metadata. No scaling, no polar unwrap etc."""
    # skimage delegates .png reading to PIL and PIL's default settings
    # suspect a decompression bomb for large ARGOS line scan images!
    # Increase the PIL threshold for decompression bomb prevention.
    # see https://github.com/napari/napari/issues/652#issuecomment-549192671
    from PIL import Image  # type: ignore

    Image.MAX_IMAGE_PIXELS = 10000000000
    zip_path = zipfile.Path(archive_file)
    zipped_files = list(zip_path.iterdir())
    image_files = list(
        filter(
            lambda f: Path(f.name).suffix.lower() in (".tif", ".png", ".bmp", ".jpg"), zipped_files
        )
    )
    sorted_image_files = sorted(image_files, key=lambda p: p.name)
    if not len(sorted_image_files):
        return []
    stack = np.stack(
        [reader(io.BytesIO(image_file.read_bytes())) for image_file in sorted_image_files]
    )
    return [StackInfo(stack=stack, argos_archive_file=str(archive_file))]


def _read_argos_archive_v2(
    archive_file: typing.Union[Path, str], descriptor: dict
) -> typing.List[StackInfo]:
    archive_layers = parse_archive_descriptor_dict(descriptor, archive_file=archive_file)

    nr_stacks = len(
        list(
            groupby(
                archive_layers,
                lambda layer: (
                    layer.axes_xy,
                    layer.illumination_metadata,
                    layer.z_stack,
                ),
            )
        )
    )
    print(f"Grouping ARGOS layers yields {nr_stacks} stacks to load as napari layers.")
    grouped = groupby(
        archive_layers,
        lambda layer: (
            layer.axes_xy,
            layer.illumination_metadata,
            layer.z_stack,
        ),
    )

    return [
        read_group(group_key, list(group), argos_archive_file=archive_file)
        for group_key, group in grouped
    ]
