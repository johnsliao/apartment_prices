import json
import sys
import os.path
import subprocess
import shlex
import time

# boston area
MIN_LAT=41.5136
MAX_LAT=42.8941
MIN_LON=-72.0974
MAX_LON=-70.3163

MAX_MARKERS = 500

class AreaTooLarge(Exception):
  pass

class FetchTimeout(Exception):
  pass

_stats = {'calls': 0, 'listings': 0}

def make_search_params(minLat, minLng, maxLat, maxLng):
  return {
    "landlordUserId": 0,
    "state": "",
    "county": "",
    "city": "",
    "zip": "",
    "brand": 792,
    "minPrice": 0,
    "maxPrice": 20000,
    "hasSection8Voucher": False,
    "yearlyIncome": 0,
    "familySize": -1,
    "voucherSize": -1,
    "isExcludeExceedsEligibility": False,
    "bedCounts": "",
    "requiredMoreBedCounts": 0,
    "bathCount": 0,
    "sortExpression": "LastUpdate Desc",
    "itemsPerPage": 32,
    "page": 1,
    "returnOnlyCount": False,
    "minLivingArea": 0,
    "maxLivingArea": 0,
    "propertyTypeCategories": "",
    "inUnitWasherAndDryer": False,
    "onsiteLaundryFacilities": False,
    "balconyPatio": False,
    "parking": False,
    "fitnessCenter": False,
    "communitySwimmingPool": False,
    "airConditioning": False,
    "dishwasher": False,
    "utilitiesIncluded": False,
    "elevator": False,
    "ageRestricted": False,
    "noAgeRestrictions": False,
    "petFriendly": False,
    "leaseIncentives": False,
    "picturesOnly": False,
    "showSection8Badge": False,
    "showTrustedOwnerBadge": False,
    "incomeRestricted": False,
    "physical": False,
    "visualHearing": False,
    "keywordSearch": "",
    "returnIdsOnly": False,
    "isNewLocation": 0,
    "isIncludeMapListing": True,
    "maxLatitude": str(maxLat),
    "maxLongitude": str(maxLng),
    "minLatitude": str(minLat),
    "minLongitude": str(minLng),
    "maxMarkerToShow": MAX_MARKERS,
    "schoolId": 0,
    "NCESSchoolID": 0,
    "education": "",
    "pos": "%s,%s,%s,%s" % (maxLat, maxLng, minLat, minLng),
    "zoom": 8,
    "center": "%s,%s" % ((minLng + maxLng) / 2, (minLat + maxLat) / 2),
    "isNearOrMeetIncomeResultsOnly": False,
    "enterpriseId": ""
  }

def extract_listings(result):
  if type(result) != type({}):
    import pprint
    pprint.pprint(result)
    with open("tmp_affordablehousing.json", "w") as outf:
      json.dump(result, outf)
    raise Exception("unexpected response type: %s" % type(result))

  # mapListings: lat/lng/rent for all results in the bounding box (up to maxMarkerToShow)
  # listings: detailed data (bedrooms, address, etc.) but paginated at itemsPerPage
  map_listings = result.get("mapListings", [])
  detail_listings = result.get("listings", [])

  if type(map_listings) != type([]):
    map_listings = []
  if type(detail_listings) != type([]):
    detail_listings = []

  if not map_listings and not detail_listings:
    import pprint
    print("Could not find listings in response. Keys:")
    pprint.pprint({k: type(v).__name__ + ((' len=%d' % len(v)) if type(v) == type([]) else '')
                   for k, v in result.items()})
    with open("tmp_affordablehousing.json", "w") as outf:
      json.dump(result, outf)
    raise Exception("Could not find listings in response; saved to tmp_affordablehousing.json")

  return map_listings, detail_listings

def direct_fetch(cmd_prefix, minLat, minLng, maxLat, maxLng, it):
  args = shlex.split(cmd_prefix)
  # Strip stale session cookies (-b / --cookie): they expire quickly and cause
  # the Azure App Gateway WAF to silently hang the connection instead of responding.
  # The SearchListings API works fine without session cookies.
  clean_args = []
  skip_next = False
  for arg in args:
    if skip_next:
      skip_next = False
      continue
    if arg in ('-b', '--cookie'):
      skip_next = True
      continue
    clean_args.append(arg)
  args = clean_args
  args.append("--data-raw")
  args.append(json.dumps({"searchParameters": make_search_params(minLat, minLng, maxLat, maxLng)}))
  args.append('--compressed')
  args.append('-sS')
  args.append('--max-time')
  args.append('30')
  print('  sleeping 1s before request...')
  time.sleep(1)
  print('  sending request...')
  try:
    raw = subprocess.check_output(args, stderr=subprocess.PIPE)
  except subprocess.CalledProcessError as e:
    print('  curl failed with exit code %d' % e.returncode)
    print('  stderr: %s' % e.stderr.decode('utf-8', errors='replace')[:500])
    if e.returncode == 28:  # curl timeout
      raise FetchTimeout()
    raise
  print('  got response (%d bytes)' % len(raw))
  try:
    result = json.loads(raw)
  except json.JSONDecodeError:
    print('  response is not JSON. First 500 chars:')
    print('  ' + raw.decode('utf-8', errors='replace')[:500])
    raise Exception("non-JSON response from server (session expired? redirect?)")
  map_listings, detail_listings = extract_listings(result)

  _stats['calls'] += 1
  _stats['listings'] += len(map_listings)
  print('  [call #%d: +%d map, +%d detail, %d total so far]' % (
    _stats['calls'], len(map_listings), len(detail_listings), _stats['listings']))

  if len(map_listings) >= MAX_MARKERS:
    if it > 20:
      import pprint
      pprint.pprint(map_listings[:3])
    else:
      raise AreaTooLarge()

  return map_listings, detail_listings

def intermediate(minVal, maxVal):
  return (maxVal-minVal)/2 + minVal

def fetch(cmd_prefix, minLat, minLng, maxLat, maxLng, it=0):
  print('%sfetching bbox: lat=[%.6f, %.6f] lng=[%.6f, %.6f] (depth %d)' %
        ('  ' * it, minLat, maxLat, minLng, maxLng, it))

  def fetchHelper(minLat, minLng, maxLat, maxLng):
    return fetch(cmd_prefix, minLat, minLng, maxLat, maxLng, it+1)

  try:
    return direct_fetch(cmd_prefix, minLat, minLng, maxLat, maxLng, it)
  except (AreaTooLarge, FetchTimeout) as e:
    split_dir = 'lat' if it % 2 else 'lng'
    reason = 'timeout' if isinstance(e, FetchTimeout) else 'too many results'
    print('%s  %s, splitting on %s (depth %d)' % ('  ' * it, reason, split_dir, it))
    if it % 2:
      m1, d1 = fetchHelper(minLat, minLng, intermediate(minLat, maxLat), maxLng)
      m2, d2 = fetchHelper(intermediate(minLat, maxLat), minLng, maxLat, maxLng)
    else:
      m1, d1 = fetchHelper(minLat, minLng, maxLat, intermediate(minLng, maxLng))
      m2, d2 = fetchHelper(minLat, intermediate(minLng, maxLng), maxLat, maxLng)
    return (m1 + m2, d1 + d2)

def download(fname, curl_file):
  print('reading curl from %s' % curl_file)
  with open(curl_file) as f:
    inp = f.read()
  inp = " ".join(l.rstrip().rstrip("\\") for l in inp.splitlines() if l.strip())

  print('got input (%d chars)' % len(inp))

  if "--data-raw" not in inp:
    raise Exception("Something looks wrong.  Was that the curl version of a SearchListings request?")

  cmd_prefix = inp.split("--data-raw")[0]
  print('extracted curl prefix (%d chars), starting fetch...' % len(cmd_prefix))
  map_listings, detail_listings = fetch(cmd_prefix, MIN_LAT, MIN_LON, MAX_LAT, MAX_LON)
  if not map_listings:
    raise Exception("no response")

  # dedup by CommunityId; prefer detail listings (richer data w/ bedrooms)
  by_id = {}
  for m in map_listings:
    cid = m["CommunityId"]
    by_id[cid] = m
  for d in detail_listings:
    cid = d["CommunityId"]
    by_id[cid] = d  # overwrites map entry with richer detail
  result = list(by_id.values())

  print('fetch complete: %d map + %d detail -> %d unique listings' % (
    len(map_listings), len(detail_listings), len(result)))
  with open(fname, 'w') as outf:
    json.dump(result, outf, indent=2)
  print('saved deduped JSON to %s' % fname)

def process(fname_in, fname_out):
  with open(fname_in) as inf:
    data = json.loads(inf.read())

  processed = []
  for listing in data:
    lat = listing["Latitude"]
    lon = listing["Longitude"]
    rent = listing.get("MinRent") or listing.get("MinAskingRent") or 0
    bedrooms = listing.get("MinBedroomCount", "")
    apt_id = listing["CommunityId"]

    processed.append((rent, bedrooms, apt_id, lon, lat))

  print('processed %d listings (%d with bedrooms)' % (
    len(processed), sum(1 for r, b, *_ in processed if b != "")))
  with open(fname_out, "w") as outf:
    for rent, bedrooms, apt_id, lon, lat in processed:
      outf.write("%s %s %s %s %s\n" % (rent, bedrooms, apt_id, lon, lat))
  print('wrote %s' % fname_out)

def start(curl_file, fname_download, fname_processed):
  if not os.path.exists(curl_file):
    raise Exception("%s not found. Save your curl command to a text file first." % curl_file)

  if not os.path.exists(fname_download):
    download(fname_download, curl_file)
  if not os.path.exists(fname_download):
    raise Exception("%s still missing" % fname_download)

  if not os.path.exists(fname_processed):
    process(fname_download, fname_processed)
  else:
    print("%s already exists" % fname_processed)

  print("Now you want to use draw_heatmap.py on %s" % fname_processed)

if __name__ == "__main__":
  start(*sys.argv[1:])
