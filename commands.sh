# Query Padmapper data
python3 query_padmapper.py south-shore-download-04-01-26.json south-shore-processed-04-01-26.txt

# Query affordable housing data
python3 query_affordablehousing.py curl.txt affordablehousing-$(date +%s).json affordablehousing-$(date +%s).txt