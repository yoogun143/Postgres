# Postgres

# Squash commit before making using Git graph
For example, there are 5 commits in a branch E.g: KAN-49. 
1. You need to check out the last commit.
2. On git graph right click on the last commit of main branch and click "Reset current branch to this commit". Choose option: "Mixed. Keep working tree but reset index". The 5 local commits in KAN-49 will be squashed.
3. Right click on last commit in remote branch and delete the remote branch
4. Push the squashed branch to remote

# Cannot delete a local branch
```bash
git rebase --abort
```

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