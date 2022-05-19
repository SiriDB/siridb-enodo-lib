
# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).
 
## [Unreleased] - yyyy-mm-dd

## [0.2.29] - 2022-05-19

### Changed
- Implemented custom client asyncio protocol

### Added
- Implemented reconnect in client when haven't received heartbeat response from hub for a timespan of `2 * heartbeat_interval`
- Seperate logger for client

## [0.2.28] - 2022-05-18

### Added
- Added functionality to receive siridb's `time_precision` value for both db's and make this available for the modules
- Added optional `value_type` property for the `EnodoModuleArgument`

## [0.2.27] - 2022-05-17

### Fixed
- Fixed socket connection managing

## [0.2.26] - 2022-05-16

### Fixed
- Fixed socket data reading

### Changed
- Implemented module version specification in module property of job configs
- Changed client to asyncio's open_connection

## [0.2.25] - 2022-05-16

### Added
- Added check function to see if given dict of params is conform to EnodoModule specs
- Check if a jobs config_name contains any spaces

## [0.2.24] - 2022-05-12

### Added
- Implemented tail function of siridb for limiting datapoints fetched

### Changed
- Base model remove outliers from dataframe. Support flat lines

## [0.2.23] - 2022-05-12

### Added
- Added logging with worker version and lib version on startup of worker

## [0.2.22] - 2022-05-11
  
### Fixed
- Changed properties in siridb config comparing
  
## [0.2.21] - 2022-05-11
  
### Fixed
- Changed properties in siridb config comparing

## [0.2.20] - 2022-05-11
  
### Fixed
- Changed username to user in siridb config comparing

## [0.2.19] - 2022-05-11

### Fixed
- Fetching enodo_id from file bugfix
- Comparing config sections, env support

## [0.2.17] - 2022-05-09

### Fixed
- Client and build clean

## [0.2.16] - 2022-05-09

### Changed
- Refactored structure

### Added
- Support for Enodo ID via ENV variable `ENODO_ID`

## [0.2.15] - 2022-05-02

### Added
- Added job config property max_n_points

## [0.2.13] - 2022-04-29

### Fixed
- Fix requirements

## [0.2.10] - 2022-04-13

### Changed
- Cleanup
- Renamed model to module

## [0.2.7] - 2022-03-30

### Changed
- Changed job response structure and valdiation

## [0.2.5] - 2022-03-25

### Fixed
- Fixed missing argument for validate_data method in response base class

## [0.2.4] - 2022-03-23

### Added
- Added support for a module's job load weight data

## [0.2.3] - 2022-03-22

### Changed
- Project data

## [0.2.2] - 2022-03-22

### Added
- Data validation for response classes
- Static rules response class
