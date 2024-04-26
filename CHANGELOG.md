# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Automated publishing to PyPI upon release. PyPI is now a trusted publisher. Now you don't
  need to be a maintainer on PyPI anymore to publish a release.

## [0.2.1] - 2024-03-02

### Added

- GitHub Action to build and push Docker image

### Changed

- Small fixes in documentation and docstrings
- Dockerfile includes extra packages

## [0.2.0] - 2024-02-02

### Added

- User guide
- Download data through Appeears
- Dockerfile

### Changed

- More standardization of data classes
- R scripts run through command line call, no longer direct dependencies
- Simplified license
- Updated structure of documentation

### Removed

- Direct R calls through Rpy2
- Executing models no longer part of workflow / package

## [0.1.0] - 2023-03-05

### Added

- Interface for datasets
- Downloaders and loaders for Daymet, PPO, NPN, PEP725, Phenocam, and MODIS data
- Package metadata, citation information
- Documentation
- License information

[Unreleased]: https://github.com/phenology/springtime/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/phenology/springtime/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/phenology/springtime/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/phenology/springtime/releases/tag/v0.1.0
