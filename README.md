# mri-actor-utils

-----

## Installation

```console
pip install git+https://github.com/a2cps/mri-actor-utils.git
```

## Actor Design

This package provides a standardized way of creating actors for the A2CPS [mri_imaging_pipeline](https://github.com/a2cps/mri_imaging_pipeline). The strategy is based on the `Reactor` class, which provides access to several tapis actor boilerplate details. To create a new actor, define a subclass that inherits from `Reactor` (like `MRIQCReactor`),
