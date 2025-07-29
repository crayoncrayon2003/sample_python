```
python3.12 -m venv env
source env/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```


prefect-etl-project/
├─ ETL1/
│  ├─ 00_flows/
│  ├─ 01_data/
│  │  ├─ input/
│  │  ├─ working/
│  │  └─ output/
│  ├─ 02_metadata/
│  ├─ 03_templates/
│  ├─ 04_queries/
│  └─ config.yml
├─ ETL2/
│  └─ ...
└─ scripts
　 ├─ tasks
   │  ├─ extract/
   │  ├─ load/
   │  ├─ transform/
   │  ├─ validation/
　 └─ utils.py
