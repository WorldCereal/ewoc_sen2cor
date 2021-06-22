# EWoC Sen2cor
 Dockerfile based on Sen2cor 2.9 
## Usage

Build
```bash
docker build -t s2c .
```
Run
```bash
docker run -ti --rm -v local_volume/:/work s2c run_s2c.py /work/<S2_SAFE> /work/<out_dir>
```
This command will run sen2cor on input L1C S2 and convert the result into ewoc format 

The run_s2c.py script is temporary it will be soon updated/improved

## TODO
- [X] Run sen2cor in docker container

- [X] Use SRTM

- [ ] Get S2 L1C from creodias using dataship with an S2 L1C id 

- [ ] Accept json workplan as an input

- [ ] Upload S2 L2A ard directly to creodias bucket

- [ ] Connect to the ewoc postgresql database
