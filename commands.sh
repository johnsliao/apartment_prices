# Query Padmapper data
python3 query_padmapper.py curl_padmapper.txt south-shore-download-$(date +%m-%d-%y).json south-shore-processed-$(date +%m-%d-%y).txt

# Query affordable housing data
python3 query_affordablehousing.py curl.txt affordablehousing-$(date +%s).json affordablehousing-$(date +%s).txt