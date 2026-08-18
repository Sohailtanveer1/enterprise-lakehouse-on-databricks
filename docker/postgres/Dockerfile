FROM postgres:16-alpine

# Scripts in this directory run once, in filename order, on first init only.
# Wiping the pgdata volume is what re-runs them:
#   docker compose down -v
COPY init/ /docker-entrypoint-initdb.d/
