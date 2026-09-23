# Seeds a SQLite database from the sample CSV, so candidates start from a
# queryable data source instead of writing a CSV loader themselves.
FROM alpine:3.20
RUN apk add --no-cache sqlite
COPY encounters.csv /seed/encounters.csv
COPY seed.sh /seed/seed.sh
RUN chmod +x /seed/seed.sh
ENTRYPOINT ["/seed/seed.sh"]
