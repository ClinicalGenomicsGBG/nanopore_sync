# Nanopore sync

Watch a directory for nanopore sequencing runs and sync them to a different location on completion.

# Usage

```bash
nanopore-sync --source /path/to/source --destination /path/to/destination
```

# Options

| Option                        | Description                                                   | Default Value                                |
|-------------------------------|---------------------------------------------------------------|----------------------------------------------|
| `--source`                    | The directory to watch for new nanopore sequencing runs.      |                                              |
| `--destination`               | The directory to sync completed runs to.                      |                                              |
| `--verify/--no-verify`        | Verify the integrity (total size) of the files after syncing. | `--verify`                                   |
| `--run-name-pattern`          | Regex pattern to match nanopore run names.                    | `[0-9]{8}_[0-9]{4}_[^_]+_[^_]+_[a-f0-9]{8}`. |
| `--completion-signal-pattern` | Regex pattern to match the completion signal file.            | `.*\/final_summary.*\.txt$`                  |
| `--help`                      | Show this message and exit.                                   |                                              |