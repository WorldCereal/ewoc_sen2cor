# EWoC Sen2cor
 Dockerfile based on Sen2cor 2.9 
## Usage

Build
```bash
docker build -t ewoc_s2c .
```
Parameters:
```bash
Usage: run_s2c.py s2c [OPTIONS]

  Sen2cor

Options:
  -p, --plan TEXT       EWoC Plan in json format
  -o, --l2a_dir TEXT    Output directory
  -cfg, --config TEXT   EOdag config file
  -pv, --provider TEXT  Data provider
  --help                Show this message and exit.
```
Run
```bash
 docker run -ti --rm -v Data:/work ewoc_s2c run_s2c.py s2c -p /work/SEN2TEST/arg_21HTC.json -o /work/SEN2TEST/OUT/ -cfg /work/SEN2TEST/eodag_config.yml
```
This command will run sen2cor on input L1C S2 and convert the result into ewoc format. The level 1 data is **automatically downloaded** from the data provider using dataship

🚧 The run_s2c.py script is temporary it will be soon updated/improved

Sen2cor aux data:

- DEM: srtm tiles are automatically downloaded by sen2cor from [CGIAR](http://srtm.csi.cgiar.org/wp-content/uploads/files/srtm_5x5/TIFF/)
- **Not used** ESA CCI package can be added to this container in order to improve the atmospheric correction. This package is quite heavy for GH (5Gb), so the archive need to be downloaded in the container. Be aware that the docker image size will be around 8 Gb. 
To use the ESA CCI package, you'll need to download it from [here](http://maps.elie.ucl.ac.be/CCI/viewer/download.php), add it to this folder and uncomment the corresponding lines in the Dockerfile.


## TODO
- [X] Run sen2cor in docker container

- [X] Use SRTM
- [X] Convert output to EWoC ARD format
- [ ] Adapt dataship `get_srtm` for sen2cor (use s3 bucket, creo or custom)
- [ ] Update `run_s2c.py` or create a python package using [pyscaffold](https://github.com/pyscaffold/pyscaffold)

- [X] Get S2 L1C from creodias using dataship with an S2 L1C id 

- [X] Accept json workplan as an input

- [ ] Upload S2 L2A ard directly to creodias bucket

- [ ] Connect to the ewoc postgresql database
