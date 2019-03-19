# MammothTest
Coding test for Mammoth eng

# Setup 

After cloning the repo simply run from within the project dir:

```
virtualenv -p python3 venv
venv/lib/activate
pip install -r requirements.txt
cp _config.json config.json
```

Then input your auth token into the config file and set the extractor settings to your requirments. 

Note: the output directory must exist.

Then run:

```
./extract
```

To extract the "GSOM" dataset from Jan 1, 2018 to Jan 7, 2018


