# EWoC Sen2cor
 Dockerfile based on Sen2cor 2.9
## Usage

Build
```bash
docker build -t ewoc_s2c .
```
Run

*s2c_plan*: Process an entire plan
```bash
 docker run -ti --rm --env-file /home/ewoc_user/env.dev -v /local_folder/work/:/work ewoc_s2c:0.8.4 s2c --verbose v s2c_plan -p /work/21HTC_plan.json --data_source aws --production_id <some_id>
```
In order to read the json plan you will need to mount a volume to the container

*s2c_id*: Process one S2 L1C product

```bash
docker run -ti --rm --env-file /home/ewoc_user/env.dev ewoc_s2c:0.8.4 s2c --verbose v s2c_id -p S2B_MSIL1C_20190822T105629_N0208_R094_T30SWF_20190822T131655 --data_source creodias --production_id <some_id>
```
This command will run sen2cor on input L1C S2 and convert the result into ewoc format. The level 1 data is **automatically downloaded** from the data provider using dataship

The `--env-file` is used to environment variables to the `ewoc_s2c`container in order to upload the ARD result to s3 bucket.

**Options**

- The `--data_source` parameter can accept any value compatible with `ewoc_dag` (aws, creodias, ...)
- The `--only_scl` parameter will constraint the sen2cor processing to the Scene Classification map

Sen2cor aux data:

- DEM: srtm tiles are automatically downloaded by `ewoc_dag` from aws public or private S3 buckets
- **Not used** ESA CCI package can be added to this container in order to improve the atmospheric correction. This package is quite heavy for GH (5Gb), so the archive need to be downloaded in the container. Be aware that the docker image size will be around 8 Gb.
To use the ESA CCI package, you'll need to download it from [here](http://maps.elie.ucl.ac.be/CCI/viewer/download.php), add it to this folder and uncomment the corresponding lines in the Dockerfile.
