# LFI
We will build a Large File Injector into databases (Postgres for now)

## Project Progress
- [x] Class to connect to the database using a config file under this pattern
- [x] Split large files to multiple files and then itirate over them, keeping track in the monitoring table

```ini
[database]
host: localhost
dbname: postgres
user: postgres
password: postgres
port: 5433
```