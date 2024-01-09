"""
Napari reader plugin for DIOPTIC Argos Archives

Uses lazy loading using dask.
"""
from __future__ import annotations

import typing
from pathlib import Path

from napari import current_viewer
from napari.types import LayerDataTuple

from napari_argos_archive_reader.argos_archive_reader import (
    StackInfo,
    read_argos_archive,
)

from .synchronize import activate_synchronization


def napari_get_reader(path):
    """A basic implementation of a Reader contribution.

    Parameters
    ----------
    path : str or list of str
        Path to file, or list of paths.

    Returns
    -------
    function or None
        If the path is a recognized format, return a function that accepts the
        same path or list of paths, and returns a list of layer data tuples.
    """
    if isinstance(path, list):
        path = path[0]

    # if we know we cannot read the file, we immediately return None.
    if not path.endswith(".zip"):
        return None

    # otherwise we return the *function* that can read ``path``.
    return reader_function


def reader_function(path):
    """Take a path or list of paths and return a list of LayerData tuples.

    Readers are expected to return data as a list of tuples, where each tuple
    is (data, [add_kwargs, [layer_type]]), "add_kwargs" and "layer_type" are
    both optional.

    Parameters
    ----------
    path : str or list of str
        Path to file, or list of paths.

    Returns
    -------
    layer_data : list of tuples
        A list of LayerData tuples where each tuple in the list contains
        (data, metadata, layer_type), where data is a numpy array, metadata is
        a dict of keyword arguments for the corresponding viewer.add_* method
        in napari, and layer_type is a lower-case string naming the type of
        layer. Both "meta", and "layer_type" are optional. napari will
        default to layer_type=="image" if not provided
    """
    # handle both a string and a list of strings
    path = Path(path) if isinstance(path, str) else Path(path[0])
    print(f"Reading {path} using ARGOS Reader")

    if path.suffix.lower() != ".zip":
        raise ValueError("Expected .zip suffix for ARGOS archive source.")

    napari_stacks = read_argos_archive(path)

    def _napari_stack_info_to_layer_tuple(
        napari_stack_info: StackInfo,
    ) -> typing.List[LayerDataTuple]:
        """Generate a LayerDataTuple from StackInfo"""
        # TODO: turn this into a method of StackInfo ?
        layer_type = "image"
        add_kwargs: dict[str, typing.Any] = {}
        if napari_stack_info.name is not None:
            add_kwargs["name"] = napari_stack_info.name  # TODO: we pass the name, and it makes
            # it into the LayerDataTuple, but napari still doesn't name the file correctly.
            # This seems to be a bug in napari
        if napari_stack_info.translate is not None:
            add_kwargs["translate"] = napari_stack_info.translate
        if napari_stack_info.scale is not None:
            add_kwargs["scale"] = napari_stack_info.scale
        if napari_stack_info.metadata is not None:
            add_kwargs["metadata"] = napari_stack_info.metadata
        else:
            add_kwargs["metadata"] = {}
        if napari_stack_info.argos_archive_file is not None:
            add_kwargs["metadata"]["argos_archive_file"] = napari_stack_info.argos_archive_file
        layer_data_tuples = [(napari_stack_info.stack, add_kwargs, layer_type)]
        if napari_stack_info.segmentation is not None:
            # ARGOS segmentation images are not labels, but multiple binary masks stuffed
            # into a byte array.
            # Each bit in the segmentation image is a flag for a certain defect class.
            # For now, we simply add this as a labels layer, which will result in different
            # defect types being displayed in different colors.
            layer_data_tuples.append((napari_stack_info.segmentation, add_kwargs, "labels"))
        return layer_data_tuples

    napari_layer_tuples = []
    for napari_stack_info in napari_stacks:
        napari_layer_tuples += _napari_stack_info_to_layer_tuple(napari_stack_info)

    viewer = current_viewer()
    viewer.bind_key("s", activate_synchronization, overwrite=True)
    print("Registering keybinding: Press `s` for layer synchronization")

    return napari_layer_tuples
