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
