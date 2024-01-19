# mepdb

This project aims to build a dataset of Members of the European Parliament (MEPs) (similar to the seemingly https://journals.sagepub.com/doi/10.1177/1465116508099764). It scrapes data from the official European Parliament MEP register and stores it in a SQLite database as well as in csv format (optional) for easy access and analysis.

The dataset is structured as follows:

- `data/mep.db`: SQLite database containing the MEP data (`meps`, `attributes` and `roles` tables)
- `data/attributes.csv` / `attributes` table: Table containing MEP attributes
- `data/roles.csv` / `roles` table: Table containing MEP roles (e.g. committee memberships)

# Usage

Eventually, the dataset will be updated regularly made available for download. For now you can use a legacy verstion stoed in the `data` folder or you can build it yourself.

To build the dataset, run the following command:

1. Install dependencies

```bash
pip install -r requirements.txt
```

2. Configure config.ini

```bash
cp config.ini.example config.ini
```

3. Run the full project (warning this will take a while and make a lot of requests to the European Parliament website)

```bash
python build_mepdb.py
```
