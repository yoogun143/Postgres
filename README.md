# Postgres
This is the Postgres live locally, getting data directly from VND api and do ETL job to database
This is change to current branch

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

# Restore
1. Download the backup file, open a `cmd` command in backup file location

2. Delete current `algotrade` database
```bash
psql -U postgres -c "drop database algotrade"
```

3. Create new  `algotrade` database
```bash
psql -U postgres -c "create database algotrade"

psql -U postgres algotrade
# algotrade=#
drop schema public;
```

4. Restore backup
```bash
psql -U postgres algotrade < algotrade_20240717
```