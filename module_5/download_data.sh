set -e

TAXI_TYPE=$1 # yellow or green
YEAR=$2 # 2020 or 2021

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data/${TAXI_TYPE}_tripdata_"

for MONTH in {1..12}; do
  FORMAT_MONTH=$(printf "%02d" "$MONTH")
  URL="${URL_PREFIX}${YEAR}-${FORMAT_MONTH}.parquet"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FORMAT_MONTH}"
  LOCAL_FILE_PATH="${LOCAL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FORMAT_MONTH}.parquet"

  mkdir -p "$LOCAL_PREFIX"

  wget -O "${LOCAL_FILE_PATH}" "${URL}"
done
