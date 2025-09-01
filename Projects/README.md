# Group assignment

This repository contains the starting code for the group assignment in the course COMP.CS.320 Data-Intensive Programming, Fall 2024.

Read-only Databricks notebooks at the Databricks environment:

- [Assignment with Scala](https://adb-7895492183558578.18.azuredatabricks.net/editor/notebooks/2713031912175975?o=7895492183558578)
- [Assignment with Python](https://adb-7895492183558578.18.azuredatabricks.net/editor/notebooks/2713031912176011?o=7895492183558578)
- [Example outputs](https://adb-7895492183558578.18.azuredatabricks.net/editor/notebooks/2713031912176047?o=7895492183558578)

Importable Databricks notebooks for the group assignment:

- [Assignment with Scala](./Assignment-scala.scala)
- [Assignment with Python](./Assignment-python.py)
- [Example outputs](./Assignment-example-outputs.scala)

Locally viewable documents in Markdown format:

- [Task instructions](./Assignment-tasks.md)
- [Example outputs](./Assignment-example-outputs.md)

For local development (without Databricks):

- [Assignment Scala project](./scala)
- [Assignment Python project](./python)


# Note on how to pull from assignment
```
git stash
git pull assignment main
git stash pop
```

# Note on how to push changes to remote (IMPORTANT!!!)
```
git pull (or pull changes)
git push
```

## Commit message format (IMPORTANT!!!)
Push the changes for EVERY exercises (so that if something went wrong, we can fix it)
```
git commit -m "
Add ex1
Assignment-scala
"
```
or
```
git commit -m "
Update ex1
Assignment-python
"
```