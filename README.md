# Postgres
This is the Postgres live locally, getting data directly from VND api and do ETL job to database

# Update .gitignore file
```bash
git rm -r --cached .
git add .
git commit -m "Drop files from .gitignore"
```

# Backup database
```bash
pg_dump -U postgres algotrade > algotrade_20240717
```