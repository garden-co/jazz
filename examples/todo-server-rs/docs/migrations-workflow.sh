#!/usr/bin/env bash

# 1) Edit the tables in jazz.app.json, then check the files. validate refuses
#    a changed schema until a migration records the change.
npx jazz-tools validate

# 2) Write the migration stub, resolve any "TODO: …" it leaves, and validate
#    again: that also regenerates src/jazz_app.rs.
npx jazz-tools migrations create --name <name>
npx jazz-tools validate

# 3) Commit jazz.app.json and src/jazz_app.rs. The backend publishes the new
#    version with its permissions bundle when it next starts.
