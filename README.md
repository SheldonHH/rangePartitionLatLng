# rangePartitionLatLng
cat usss.csv | awk -F "," '{print $1" "$2" "$3" "$4}' | xargs -n4 sh -c 'redis-cli -p 6379 zadd $1 $2 "$3,$4"' sh
