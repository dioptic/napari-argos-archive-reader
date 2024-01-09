import typing
from functools import partial

from napari import Viewer
from napari.layers import Layer

ARGOS_ARCHIVE_KEY = "argos_archive_file"


def is_argos_layer(layer: Layer):
    if not hasattr(layer, "metadata"):
        return False
    if ARGOS_ARCHIVE_KEY not in layer.metadata:
        return False
    return True


def find_layers_for_archive(archive_file: str, viewer: Viewer):
    def _filter_func(layer):
        if not is_argos_layer(layer):
            return False
        if layer.metadata[ARGOS_ARCHIVE_KEY] != archive_file:
            return False
        return True

    layers = viewer.layers
    layers_for_archive = list(filter(_filter_func, layers))
    return layers_for_archive


_updating: dict[typing.Tuple[Layer], bool] = {}  # Used to


def adjust_contrast_callback(event, layer_group: typing.Tuple[Layer]):
    global _updating
    if _updating.get(layer_group, False):
        return
    _updating[layer_group] = True
    source_layer = event.source
    for layer in set(layer_group) - {source_layer}:
        # Originally tried using the following context manager
        # to block events instead of the global _updating.
        # However, I only want to block this function itself
        # from creating new events, otherwise the contrast isn't
        # actually updated. However, I don't know how to get a
        # reference to itself.
        # with layer.events.contrast_limits.blocker():
        layer.contrast_limits = source_layer.contrast_limits
    _updating[layer_group] = False


def synchronize_argos_layer(layer: Layer, viewer: Viewer):
    if not is_argos_layer(layer):
        return
    # TODO assert image layer
    layer_group = tuple(find_layers_for_archive(layer.metadata[ARGOS_ARCHIVE_KEY], viewer=viewer))
    print(
        f"Synchronizing contrast limits in layers for ARGOS archive: {layer.metadata[ARGOS_ARCHIVE_KEY]}"
    )
    callback = partial(adjust_contrast_callback, layer_group=layer_group)
    for layer in layer_group:
        layer.events.contrast_limits.connect(callback)


def activate_synchronization(viewer: Viewer):
    sel = viewer.layers.selection
    if len(sel) == 1 and is_argos_layer(layer := list(sel)[0]):
        synchronize_argos_layer(layer, viewer)
    elif len(sel) > 1:
        layer_group = tuple(sel)
        print(f"Synchronizing contrast_limits for layers {[layer.name for layer in layer_group]}")
        callback = partial(adjust_contrast_callback, layer_group=layer_group)
        for layer in layer_group:
            layer.events.contrast_limits.connect(callback)
